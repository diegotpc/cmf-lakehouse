# -----------------------------------------------------------------------------
# MÓDULO: scraper.py
# VERSIÓN: 1.1 (MVP V10)
#
# DESCRIPCIÓN:
# Este módulo es el "Obrero de Fábrica" del scraper.
#
# RESPONSABILIDAD:
# Su *única* responsabilidad es, dado un trimestre, consultar la página,
# extraer *todas* las tablas que cumplan los filtros, y devolverlas.
#
# ARQUITECTURA (V6.1):
# - Es un "Obrero Agnóstico con Filtros".
# - Acepta 'tipo_balance' (V6.1) como parámetro.
# - Filtra reportes '...05' (Separados) (V6.1).
# - Filtra usando la 'LISTA_BLANCA_CODIGOS' de config.py (V6.1).
# - Devuelve un *diccionario* de DataFrames (Extracción Pura)
#   (ej. {"[210000]": df_b, "[320000]": df_e, "[420000]": df_int})
# - Implementa la señal "RELOAD" (V6.1) para recuperación de errores.
# -----------------------------------------------------------------------------

import config
import time
import pandas as pd
import logging
import re # (V6.1) RegEx para buscar códigos [XXXXXX]
from bs4 import BeautifulSoup # El "raspador" de HTML
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select # Herramienta para <select>
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def _procesar_tabla(tabla_bs4, periodo_str):
    """
    Helper privado que procesa un <table> de BeautifulSoup.
    (V7.1: Se eliminó la lógica 'es_eerr', preserva 'Sección').

    Args:
        tabla_bs4 (bs4.element.Tag): El objeto <table> de BeautifulSoup.
        periodo_str (str): El string 'AAAA-MM' (ej. "2024-03").

    Returns:
        pd.DataFrame: Un DataFrame "largo" con las columnas:
                      ["Cuenta", "Valor", "Periodo", "Sección", "Orden"]
    """
    filas = tabla_bs4.find_all("tr") # Obtiene todas las filas <tr>
    datos = []
    seccion_actual = "Sin sección" # (V7.1) Preservado para desambiguación y formato
    orden_global = 0 # (V7.1) Preservado para el ordenamiento final

    for fila in filas:
        # (V7.1) Lógica de detección de sección: '<strong>'
        if fila.find("strong"):
            seccion_actual = fila.get_text(strip=True)
            continue # Salta al siguiente 'for fila'

        celdas = fila.find_all("td") # Obtiene todas las celdas <td>
        if len(celdas) >= 2: # Fila de datos válida
            cuenta = celdas[0].get_text(strip=True)
            
            valor = celdas[1].get_text(strip=True)
            if valor.strip() == "-":
                valor = "0"

            # Limpieza de Datos (Formato Chileno a Inglés)
            valor = valor.replace(".", "").replace(",", ".")
            try:
                valor = float(valor)
            except ValueError:
                valor = 0.0

            # (V7.1) Añade la fila con la metadata de "Sección" y "Orden"
            datos.append([cuenta, valor, periodo_str, seccion_actual, orden_global])
            orden_global += 1

    if not datos:
        return None
        
    return pd.DataFrame(datos, columns=["Cuenta", "Valor", "Periodo", "Sección", "Orden"])

# --- (V6.1) FIRMA DE FUNCIÓN MODIFICADA ---
def consultar_trimestre(driver, mes, anio, tipo_balance):
    """
    (V6.1) Consulta un trimestre y extrae TODAS las tablas [XXXXXX]
    que estén en la LISTA_BLANCA_CODIGOS y no terminen en '05'.

    Args:
        driver (webdriver.Chrome): La instancia del navegador.
        mes (str): El mes (ej. "03").
        anio (int): El año (ej. 2024).
        tipo_balance (str): "C" (Consolidado) o "I" (Individual).

    Returns:
        dict: Un diccionario de DataFrames (ej. {"[210000]": df_b, ...})
        str: O la señal "RELOAD" si el sistema falló.
    """
    # (V6.1) "try...except" general para el "Modo Zombie" (Fallo Tipo 2B)
    try:
        logging.info(f"\n📅 Consultando trimestre {mes}/{anio} (Tipo: {tipo_balance})...")
        driver.execute_script("window.scrollTo(0, 500);")

        # --- Interacción con Dropdowns (V6.1) ---
        Select(driver.find_element(By.ID, "mm")).select_by_value(mes)
        Select(driver.find_element(By.ID, "aa")).select_by_value(str(anio))
        Select(driver.find_element(By.NAME, "tipo")).select_by_value(tipo_balance) # (V6.1) Parámetro
        Select(driver.find_element(By.NAME, "tipo_norma")).select_by_value(config.TIPO_NORMA)

        time.sleep(config.DELAY_CONSULTA)
        driver.find_element(By.CSS_SELECTOR, "input[type='submit'][value='Consultar']").click()
        
        # (V4) Pausa Crítica: Esperamos que el AJAX de la CMF refresque la página.
        # No podemos usar WebDriverWait porque la URL no cambia.
        time.sleep(config.DELAY_ESPERA_CONSULTA)

        # --- Triaje de Respuesta: FALLO TIPO 1 (No hay datos) ---
        if driver.page_source.find("No existe información de la entidad") != -1:
            logging.warning("⚠️ No hay información para este periodo.")
            try:
                # "Botón Atrás" para salir de la página de error.
                driver.execute_script("window.history.go(-1);")
                time.sleep(1)
            except Exception:
                pass 
            # (V6.1) Devolver un dict vacío (éxito, 0 datos).
            return {}

        # --- Triaje de Respuesta: ÉXITO (Extracción Pura V6.1) ---
        soup = BeautifulSoup(driver.page_source, "html.parser")
        tablas = soup.find_all("table")

        tablas_encontradas = {} # { "[210000]": <objeto_tabla_bs4>, ... }
        
        for tabla in tablas:
            th = tabla.find("th")
            if not th: continue
            titulo = th.text.strip()

            # (V6.1) Filtro 1: Ignorar reportes 'Separados' (...05)
            if titulo.endswith("05]"):
                logging.debug(f"Ignorando reporte 'Separado' (05): {titulo}")
                continue

            # (V6.1) RegEx para encontrar CUALQUIER código [XXXXXX]
            match = re.search(r'\[(\d{6})\]', titulo)
            if match:
                codigo = f"[{match.group(1)}]"
                # (V6.1) Log legible usando la taxonomía completa
                nombre_tabla = config.TAXONOMIA_CMF.get(codigo, f"Código {codigo}")

                # (V6.1) Filtro 2: Usar la Whitelist
                if codigo not in config.LISTA_BLANCA_CODIGOS:
                    logging.info(f"🧬 Ignorando tabla no soportada (no en Whitelist): {nombre_tabla}")
                    continue
                
                # Si pasa ambos filtros, se añade a la lista de procesamiento
                if codigo not in tablas_encontradas:
                    tablas_encontradas[codigo] = tabla
                    # (V6.1) Log de éxito de extracción
                    logging.info(f"🧬 Tabla encontrada (en Whitelist): {nombre_tabla}")

        periodo_str = f"{anio}-{mes}"
        
        # 2. Procesar solo las tablas encontradas ("Handoff" a Pandas)
        resultados_dict = {} # { "[210000]": <objeto_dataframe>, ... }
        
        if not tablas_encontradas:
            logging.warning("⚠️ No se encontró ninguna tabla en la Whitelist este periodo.")
        
        for codigo, tabla_html in tablas_encontradas.items():
            nombre_tabla = config.TAXONOMIA_CMF.get(codigo, f"Código {codigo}")
            logging.debug(f"Procesando tabla: {nombre_tabla}")
            
            # (V7.1) Llama al helper (que preserva "Sección")
            df = _procesar_tabla(tabla_html, periodo_str)
            if df is not None:
                resultados_dict[codigo] = df
        
        # --- (V6.1) Fin de Extracción ---

        # --- Triaje de Respuesta: FALLO TIPO 2A (Post-Scrape) ---
        try:
            link_back = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.LINK_TEXT, "Información Financiera"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link_back)
            time.sleep(config.DELAY_NAVEGACION_CORTA)
            link_back.click()
        except Exception as e:
            # Si el clic de "volver" falla (sesión muerta), lo
            # registramos como ERROR (Nivel 4).
            logging.error(f"❌ No se pudo volver a 'Información Financiera': {e}")
            # (V6.1) Devolver solo la señal "RELOAD"
            return "RELOAD" 

        # (V6.1) Devolver el diccionario de DFs procesados
        return resultados_dict

    # --- Triaje de Respuesta: FALLO TIPO 2B (Pre-Scrape / "Modo Zombie") ---
    except Exception as e:
        # Si CUALQUIER cosa falló (ej. no encontró 'mm'), caemos aquí.
        logging.error(f"⚠️ Trimestre {mes}/{anio} no disponible o falló la extracción: {e}")
        # (V6.1) Devolver solo la señal "RELOAD"
        return "RELOAD"