# -----------------------------------------------------------------------------
# MÓDULO: driver_setup.py
# VERSIÓN: 1.3 (Lakehouse / Parche Viewport + Parche Anti-Bot)
# -----------------------------------------------------------------------------

import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager 

def setup_driver(headless=False):
    logging.info("🚀 Inicializando WebDriver (Modo Auto-Gestionado)...")

    try:
        chrome_options = Options()
        
        # --- Argumentos de Estabilidad ---
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--window-size=1920,1080") 

        # --- PARCHE ANTI-BOT (Mandatorio para CMF) ---
        # 1. Sobrescribir el User-Agent para ocultar "HeadlessChrome"
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        # 2. Desactivar la bandera interna 'navigator.webdriver'
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        # 3. Ocultar extensiones de automatización de la interfaz
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        if headless:
            chrome_options.add_argument("--headless")
            logging.info("WebDriver inicializado en modo Headless (Con Enmascaramiento).")
        else:
            logging.info("WebDriver inicializado en modo Visible.")

        logging.info("Verificando/Descargando chromedriver con webdriver-manager...")
        service = Service(ChromeDriverManager().install())
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 4. Parche final de evasión mediante inyección de script CDP (Chrome DevTools Protocol)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
            """
        })
        
        logging.info("✅ WebDriver inicializado exitosamente.")
        return driver
        
    except Exception as e:
        logging.critical(f"❌ Error crítico al inicializar WebDriver: {e}")
        return None