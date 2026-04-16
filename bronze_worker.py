import os
import logging
import psycopg2
from dotenv import load_dotenv

import config
import driver_setup
import navigation
import scraper
import cloud_storage

# Configuración estricta solicitada
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # 1. Cargar credenciales
    load_dotenv()
    db_url = os.getenv("SUPABASE_DB_URL")
    
    if not db_url:
        logger.error("SUPABASE_DB_URL no encontrada en las variables de entorno.")
        return

    conn = None
    cursor = None
    driver = None
    
    try:
        # 2. Conectar a PostgreSQL
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        # 3. Bloqueo Transaccional
        select_query = """
            SELECT id_extraccion, rut_emisor, periodo 
            FROM log_extraccion_cmf 
            WHERE estado = 'PENDIENTE' 
            LIMIT 1 FOR UPDATE SKIP LOCKED;
        """
        cursor.execute(select_query)
        registro = cursor.fetchone()
        
        # 4. Sin tareas
        if not registro:
            conn.commit()
            print("No hay tareas pendientes")
            return
            
        id_extraccion, rut_emisor, periodo = registro
        logger.info(f"Tarea obtenida: ID {id_extraccion} | RUT {rut_emisor} | Periodo {periodo}")
        
        # 5. Actualizar a EN_PROCESO
        update_proceso_query = """
            UPDATE log_extraccion_cmf 
            SET estado = 'EN_PROCESO' 
            WHERE id_extraccion = %s;
        """
        cursor.execute(update_proceso_query, (id_extraccion,))
        conn.commit()
        
        # 6. Parsear periodo
        anio_str, trimestre_str = periodo.split('-')
        anio = int(anio_str)
        mapa_trimestres = {'Q1': '03', 'Q2': '06', 'Q3': '09', 'Q4': '12'}
        mes = mapa_trimestres.get(trimestre_str)
        
        if not mes:
            raise ValueError(f"Trimestre irreconocible: {trimestre_str}")
        
        # 7. Instanciar driver (dentro del flujo para poder limpiarlo después)
        driver = driver_setup.setup_driver(headless=True)
        if not driver:
            raise RuntimeError("No se pudo iniciar el WebDriver")
            
        try:
            # 8. Navegar
            navegacion_ok = navigation.navigate_to_financial_info(driver, config.URL_BUSQUEDA_CMF, rut_emisor)
            if not navegacion_ok:
                raise Exception("Fallo en la navegación hacia la CMF")
                
            # 9. Extraer
            datos = scraper.consultar_trimestre(driver, mes, anio, "C")
            
            # 10. Validar datos y subir a R2
            if isinstance(datos, dict):
                if len(datos) > 0:
                    subida_ok = cloud_storage.upload_to_r2(datos, rut_emisor, periodo)
                    if not subida_ok:
                        raise Exception("Fallo de subida a R2")
                else:
                    logger.warning(f"Periodo {periodo} sin datos en CMF. Se omite subida a R2.")
                
                # ÉXITO TOTAL (Se marca completado tanto si hubo datos como si es un trimestre vacío)
                update_exito = """
                    UPDATE log_extraccion_cmf 
                    SET estado = 'COMPLETADO' 
                    WHERE id_extraccion = %s;
                """
                cursor.execute(update_exito, (id_extraccion,))
                
            else:
                # Caso de señal "RELOAD" o fallos estructurales
                raise Exception(f"Extracción fallida o sesión muerta. Retornó: {datos}")
                
        except Exception as e:
            # Error de Lógica, Extracción o Subida
            logger.error(f"Error procesando la tarea: {str(e)}")
            update_error = """
                UPDATE log_extraccion_cmf 
                SET estado = 'ERROR', reintentos = reintentos + 1 
                WHERE id_extraccion = %s;
            """
            cursor.execute(update_error, (id_extraccion,))
            
        finally:
            # Hacer commit de la resolución (COMPLETADO o ERROR)
            conn.commit()
            
    except Exception as e_critica:
        logger.critical(f"Error crítico a nivel base de datos u orquestación: {str(e_critica)}")
        
    finally:
        # Asegurar desconexiones mandatorias
        if driver:
            try:
                driver.quit()
            except:
                pass
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
