# -----------------------------------------------------------------------------
# MÓDULO: navigation.py
# VERSIÓN: 1.1 (MVP V10)
#
# DESCRIPCIÓN:
# Este módulo es el "Piloto" o "Navegador GPS" del scraper.
#
# RESPONSABILIDAD:
# Su *única* responsabilidad es conducir el "auto" (driver) desde
# la URL base hasta la página de consulta de "Información Financiera".
#
# ARQUITECTURA (V5):
# - Es 100% estable.
# - Recibe el 'driver', 'url_busqueda' y 'rut' como parámetros.
# - Usa 'WebDriverWait' (esperas inteligentes) en lugar de 'time.sleep'
#   para máxima velocidad y robustez.
# - Realiza el "Doble Check de Navegación" (V6.1) al final para
#   validar que los dropdowns 'mm' y 'aa' existen.
# -----------------------------------------------------------------------------

import time
import config  # Para leer los delays (V4) y la URL (V5)
import logging # Para reportar hitos y errores (V4)

# Herramientas de Selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait # Esperador Inteligente
from selenium.webdriver.support import expected_conditions as EC # Condiciones
from selenium.webdriver.common.keys import Keys # Tecla "Enter"
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def navigate_to_financial_info(driver, url_busqueda, rut):
    """
    Navega desde la página principal hasta la sección de Información Financiera
    para un RUT específico.
    
    Args:
        driver (webdriver.Chrome): La instancia del navegador.
        url_busqueda (str): La URL de la CMF (desde config.py).
        rut (str): El RUT a buscar (desde main.py/input).

    Returns:
        bool: True si la navegación fue exitosa, False si falló.
    """
    try:
        # 1. "Conducir" a la URL.
        logging.info(f"🌐 Abriendo página principal: {url_busqueda}")
        driver.get(url_busqueda)

        # 2. Buscar el campo RUT (ID: "valor").
        # WebDriverWait(driver, 20) -> Espera un MÁXIMO de 20 segundos.
        # .until(EC.presence_of_element_located(...)) -> Condición.
        # Es superior a 'time.sleep' porque continúa tan pronto
        # como el elemento aparece.
        input_fiscalizados = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "valor"))
        )
        input_fiscalizados.clear()
        input_fiscalizados.send_keys(rut)
        input_fiscalizados.send_keys(Keys.RETURN) # Presiona "Enter".
        logging.info(f"🔍 RUT '{rut}' ingresado correctamente.")

        # 3. Esperar la tabla de resultados.
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//table"))
        )

        # 4. Encontrar el link "Emisores de Valores...".
        # Este XPath es robusto (V6.1):
        # "//td[...]" -> Encuentra una celda ('td') que contenga el texto...
        # "/parent::tr" -> ...sube a la fila ('tr') que la contiene...
        # "/td/a" -> ...y baja a la celda ('td') que contiene el link ('a').
        xpath_link = "//td[normalize-space(.)='Emisores de Valores de Oferta Pública']/parent::tr/td/a"
        
        enlace_emisor = WebDriverWait(driver, 20).until(
            # 'element_to_be_clickable' es una espera MÁS FUERTE que
            # 'presence_of_element_located'. Espera que sea visible.
            EC.element_to_be_clickable((By.XPATH, xpath_link))
        )
        
        # 'execute_script' (JavaScript) es la forma más confiable
        # de hacer scroll para evitar clics "interceptados".
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", enlace_emisor)
        # (V4) Usamos el delay centralizado. Es un 'time.sleep'
        # porque esperamos una *animación* (scroll), no un elemento.
        time.sleep(config.DELAY_NAVEGACION_CORTA) 
        enlace_emisor.click()
        logging.info("✅ Encontrado 'Emisores de Valores...'. Click.")

        # 5. Clic en "Información Financiera".
        link_info = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Información Financiera"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link_info)
        time.sleep(config.DELAY_NAVEGACION_CORTA)
        link_info.click()
        logging.info("✅ Navegación a 'Información Financiera' exitosa.")
        
        # 6. --- El "Doble Check" de Navegación (CRÍTICO) ---
        # Esperamos a que los dropdowns 'mm' y 'aa' aparezcan.
        # Esta es nuestra *garantía* (V6.1) de que llegamos a la página
        # de consulta correcta y de que el "Obrero" (scraper.py) podrá trabajar.
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "mm")))
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "aa")))
        logging.info("✅ Página de consulta cargada y validada (dropdowns 'mm' y 'aa' encontrados).")
        
        return True # "Éxito"

    # 'except' específicos: si falla por 'Timeout' o 'NoEncontrado',
    # sabemos que es un error de navegación (Nivel 4).
    except (TimeoutException, NoSuchElementException) as e:
        logging.error(f"❌ Error durante la navegación: No se pudo encontrar un elemento.")
        logging.error(f"Detalle: {e}")
        return False # "Fallo"
    except Exception as e:
        # 'except' genérico para cualquier otro error (ej. JavaScript).
        logging.error(f"❌ Error inesperado durante la navegación: {e}")
        return False # "Fallo"