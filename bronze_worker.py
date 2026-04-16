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

    # Mantenemos UNA sola instancia del navegador abierta para toda la sesión
    driver = driver_setup.setup_driver(headless=True)
    if not driver:
        raise RuntimeError("No se pudo iniciar el WebDriver")

    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        while True:
            # 3. Bloqueo Transaccional
            select_query = """
                SELECT id_extraccion, rut_emisor, periodo 
                FROM log_extraccion_cmf 
                WHERE estado = 'PENDIENTE' 
                LIMIT 1 FOR UPDATE SKIP LOCKED;
            """
            cursor.execute(select_query)
            registro = cursor.fetchone()
            
            # 4. Condición de salida: Cola vacía
            if not registro:
                conn.commit()
                logger.info("No hay más tareas pendientes. Cerrando orquestador.")
                break
                
            id_extraccion, rut_emisor, periodo = registro
            logger.info(f"\n--- Iniciando Tarea: RUT {rut_emisor} | Periodo {periodo} ---")
            
            # 5. Actualizar a EN_PROCESO
            update_proceso_query = "UPDATE log_extraccion_cmf SET estado = 'EN_PROCESO' WHERE id_extraccion = %s;"
            cursor.execute(update_proceso_query, (id_extraccion,))
            conn.commit()
            
            anio_str, trimestre_str = periodo.split('-')
            anio = int(anio_str)
            mapa_trimestres = {'Q1': '03', 'Q2': '06', 'Q3': '09', 'Q4': '12'}
            mes = mapa_trimestres.get(trimestre_str)
            
            try:
                # 8. Navegar y Extraer
                navegacion_ok = navigation.navigate_to_financial_info(driver, config.URL_BUSQUEDA_CMF, rut_emisor)
                if not navegacion_ok:
                    raise Exception("Fallo en la navegación hacia la CMF")
                    
                datos = scraper.consultar_trimestre(driver, mes, anio, "C")
                
                # 10. Validar datos (Parche tolerante a vacíos)
                if isinstance(datos, dict):
                    if len(datos) > 0:
                        subida_ok = cloud_storage.upload_to_r2(datos, rut_emisor, periodo)
                        if not subida_ok:
                            raise Exception("Fallo de subida a R2")
                    else:
                        logger.warning(f"Periodo {periodo} sin datos en CMF. Se omite subida.")
                    
                    update_exito = "UPDATE log_extraccion_cmf SET estado = 'COMPLETADO' WHERE id_extraccion = %s;"
                    cursor.execute(update_exito, (id_extraccion,))
                else:
                    raise Exception(f"Extracción fallida. Retornó: {datos}")
                    
            except Exception as e:
                logger.error(f"Error procesando la tarea: {str(e)}")
                update_error = "UPDATE log_extraccion_cmf SET estado = 'ERROR', reintentos = reintentos + 1 WHERE id_extraccion = %s;"
                cursor.execute(update_error, (id_extraccion,))
                
            finally:
                conn.commit()
                # RETARDO DE CORTESÍA (Evasión de baneo): 4 a 9 segundos entre cada petición
                tiempo_espera = random.uniform(4.0, 9.0)
                logger.info(f"Pausa de cortesía: {tiempo_espera:.2f} segundos...")
                time.sleep(tiempo_espera)
                
    except Exception as e_critica:
        logger.critical(f"Error crítico de BD: {str(e_critica)}")
        
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
