import os
import logging
import psycopg2
import time
import random
from dotenv import load_dotenv

import config
import driver_setup
import navigation
import scraper
import cloud_storage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    load_dotenv()
    db_url = os.getenv("SUPABASE_DB_URL")
    
    if not db_url:
        logger.error("SUPABASE_DB_URL no encontrada en las variables de entorno.")
        return

    # Persistencia de Sesión: se reutiliza esta única VM para toda la sesión del worker
    driver = driver_setup.setup_driver(headless=True)
    if not driver:
        raise RuntimeError("No se pudo iniciar el WebDriver")

    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        while True:
            # Bloqueo Transaccional
            select_query = """
                SELECT id_extraccion, rut_emisor, periodo 
                FROM log_extraccion_cmf 
                WHERE estado = 'PENDIENTE' 
                LIMIT 1 FOR UPDATE SKIP LOCKED;
            """
            cursor.execute(select_query)
            registro = cursor.fetchone()
            
            # Condición de salida limpia por agotamiento de cola
            if not registro:
                conn.commit()
                logger.info("No existen tareas marcadas como PENDIENTE. Worker drenado cerrando.")
                break
                
            id_extraccion, rut_emisor, periodo = registro
            logger.info(f"\n--- [TAREA BRONZE] FUT: RUT {rut_emisor} | {periodo} ---")
            
            update_proceso_query = "UPDATE log_extraccion_cmf SET estado = 'EN_PROCESO' WHERE id_extraccion = %s;"
            cursor.execute(update_proceso_query, (id_extraccion,))
            conn.commit()
            
            anio_str, trimestre_str = periodo.split('-')
            anio = int(anio_str)
            mapa_trimestres = {'Q1': '03', 'Q2': '06', 'Q3': '09', 'Q4': '12'}
            mes = mapa_trimestres.get(trimestre_str)
            
            try:
                datos_totales = {}
                
                # [INTENTO EXTRACTIVO MODO: C]
                if navigation.navigate_to_financial_info(driver, config.URL_BUSQUEDA_CMF, rut_emisor):
                    datos_c = scraper.consultar_trimestre(driver, mes, anio, "C")
                    if isinstance(datos_c, dict) and datos_c:
                        for idx_codigo, val in datos_c.items():
                            datos_totales[f"C_{idx_codigo}"] = val
                else:
                    logger.warning(f"Ruta de acceso a información C. fallida para RUT: {rut_emisor}.")

                # [INTENTO EXTRACTIVO MODO: I]
                # Obligatoriamente volvemos a enviar al navegador al punto de control original
                if navigation.navigate_to_financial_info(driver, config.URL_BUSQUEDA_CMF, rut_emisor):
                    datos_i = scraper.consultar_trimestre(driver, mes, anio, "I")
                    if isinstance(datos_i, dict) and datos_i:
                        for idx_codigo, val in datos_i.items():
                            datos_totales[f"I_{idx_codigo}"] = val
                else:
                    logger.warning(f"Ruta de acceso a información I. fallida para RUT: {rut_emisor}.")
                    
                # [RESOLUCIÓN BRONZE]
                if datos_totales:
                    subida_ok = cloud_storage.upload_to_r2(datos_totales, rut_emisor, periodo)
                    if subida_ok:
                        update_exito = "UPDATE log_extraccion_cmf SET estado = 'COMPLETADO' WHERE id_extraccion = %s;"
                        cursor.execute(update_exito, (id_extraccion,))
                    else:
                        raise Exception("Disrupción asíncrona I/O nube hacia Cloudflare R2")
                else:
                    logger.warning(f"Alerta: Sin material tabular recuperable (C/I) para {periodo}. Resolviendo sin fallos.")
                    update_exito = "UPDATE log_extraccion_cmf SET estado = 'COMPLETADO' WHERE id_extraccion = %s;"
                    cursor.execute(update_exito, (id_extraccion,))
                    
            except Exception as e:
                logger.error(f"Pánico local capturado procesando tarea primaria: {str(e)}")
                update_error = "UPDATE log_extraccion_cmf SET estado = 'ERROR', reintentos = reintentos + 1 WHERE id_extraccion = %s;"
                cursor.execute(update_error, (id_extraccion,))
                
            finally:
                conn.commit()
                # Retardo de seguridad anti-baneo para firewall de aplicación remota (5 a 12 segs)
                tiempo_espera = random.uniform(5.0, 12.0)
                logger.info(f"Sleeping de evasión de cuota CMF: ~{tiempo_espera:.2f}s...")
                time.sleep(tiempo_espera)
                
    except Exception as e_critica:
        logger.critical(f"Seg fault logico de transacción matriz o de DB: {str(e_critica)}")
        
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    main()
