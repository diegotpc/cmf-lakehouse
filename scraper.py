import os
import config
import time
import logging
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def guardar_evidencia(driver, nombre_archivo):
    """
    Sistema de diagnóstico de 'Caja Negra'.
    Captura un pantallazo y el HTML actual si falla una sincronización.
    """
    if not os.path.exists("debug"):
        os.makedirs("debug")
        
    driver.save_screenshot(f"debug/{nombre_archivo}.png")
    with open(f"debug/{nombre_archivo}.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    logging.info(f"Evidencia guardada en debug/{nombre_archivo}.[png/html]")

def extraer_datos_tabla(driver):
    """
    Parsea la página actual de HTML y extrae exhaustivamente todas las tablas
    preservando esquemas. Captura Headers en 'th' y datos en 'td'.
    
    Returns:
        Un dict con la estructura {"headers": [...], "data": [[...]]}
    """
    soup = BeautifulSoup(driver.page_source, "html.parser")
    tablas = soup.find_all("table")
    all_headers = []
    all_data = []
    
    for tabla in tablas:
        filas = tabla.find_all("tr")
        for fila in filas:
            ths = fila.find_all("th")
            if ths:
                # Extraemos y poblamos headers, cuidando no sobreescribir si hay varios (toma el primero más largo)
                header_row = [th.get_text(strip=True) for th in ths]
                if not all_headers or len(header_row) > len(all_headers):
                    all_headers = header_row
            
            tds = fila.find_all("td")
            if tds:
                row_data = [td.get_text(strip=True) for td in tds]
                all_data.append(row_data)

    return {"headers": all_headers, "data": all_data}

def consultar_trimestre(driver, mes, anio, tipo_balance):
    """
    Consulta un trimestre y realiza un Bucle de Descubrimiento iterando por 
    todos los 'value' de las opciones en 'inte_id'.
    """
    try:
        logging.info(f"📅 Consultando trimestre {mes}/{anio} (Tipo: {tipo_balance})...")
        driver.execute_script("window.scrollTo(0, 500);")

        Select(driver.find_element(By.ID, "mm")).select_by_value(mes)
        Select(driver.find_element(By.ID, "aa")).select_by_value(str(anio))
        Select(driver.find_element(By.NAME, "tipo")).select_by_value(tipo_balance)
        Select(driver.find_element(By.NAME, "tipo_norma")).select_by_value(config.TIPO_NORMA)

        time.sleep(config.DELAY_CONSULTA)
        driver.find_element(By.CSS_SELECTOR, "input[type='submit'][value='Consultar']").click()
        time.sleep(config.DELAY_ESPERA_CONSULTA)

        if driver.page_source.find("No existe información de la entidad") != -1:
            logging.warning(f"⚠️ No hay información de {tipo_balance} para este periodo.")
            try:
                driver.execute_script("window.history.go(-1);")
                time.sleep(1)
            except Exception:
                pass 
            return {}

        resultados_dict = {}
        
        try:
            # Esperamos la visibilidad del select 'inte_id' para iniciar el Bucle de Descubrimiento
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "inte_id"))
            )
        except (TimeoutException, NoSuchElementException) as e:
            logging.error(f"🛑 Fallo de sincronización en 'inte_id'. Generando caja negra...")
            guardar_evidencia(driver, f"fallo_dropdown_{tipo_balance}")
            raise e
            
        select_inte = Select(driver.find_element(By.ID, "inte_id"))
        opciones_disponibles = [opt.get_attribute("value") for opt in select_inte.options]
        
        for val in opciones_disponibles:
            if str(val) == "0":
                continue # Evitar el valor nulo base
                
            logging.info(f"🔍 Bucle de Descubrimiento: Solicitando reporte {val}...")
            
            # Reconectamos el Select en cada ciclo para evadir un StaleElementReferenceException derivado del ajax
            select_obj = Select(driver.find_element(By.ID, "inte_id"))
            select_obj.select_by_value(val)
            
            # Sincronización Agnóstica y Segura (Inyección de AJAX)
            time.sleep(1.5)
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "inte_id"))
            )
            time.sleep(1.0)
            
            datos_brutos = extraer_datos_tabla(driver)
            
            if datos_brutos and (datos_brutos["data"] or datos_brutos["headers"]):
                resultados_dict[str(val)] = datos_brutos
                
        # Fin de las descargas, volver
        try:
            link_back = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.LINK_TEXT, "Información Financiera"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link_back)
            time.sleep(config.DELAY_NAVEGACION_CORTA)
            link_back.click()
        except:
            return "RELOAD"
            
        return resultados_dict

    except Exception as e:
        logging.error(f"⚠️ Caída grave extrayendo {mes}/{anio}: {e}")
        return "RELOAD"