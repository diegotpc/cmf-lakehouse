import os

# --- (V5) RUTA DE SALIDA PORTÁTIL (Mantenida por compatibilidad futura) ---
OUTPUT_DIRECTORY = os.getcwd()

# --- (V8.1) ABREVIATURAS DE TAXONOMÍA ---
TAXONOMIA_ABREVIATURAS = {
    "[210000]": "BAL_Corriente",
    "[220000]": "BAL_Liquidez",
    "[310000]": "EERR_Funcion",
    "[320000]": "EERR_Naturaleza",
    "[420000]": "EERR_Integral",
    "[510000]": "EFE_Directo",
    "[520000]": "EFE_Indirecto",
    "[610000]": "Patrimonio",
    "[200000]": "BAL_Individual",
    "[300000]": "EERR_Individual",
    "[500000]": "EFE_Individual",
    "[600000]": "Patrimonio_Ind"
}

# --- (V6.1) TAXONOMÍA ---
TAXONOMIA_CMF = {
    "[110000]": "Nota - Info General (Consolidado)",
    "[210000]": "Balance (Corriente/No Corriente)",
    "[220000]": "Balance (Liquidez)",
    "[310000]": "EERR (Por Función)",
    "[320000]": "EERR (Por Naturaleza)",
    "[420000]": "EERR Integral",
    "[510000]": "EFE (Directo)",
    "[520000]": "EFE (Indirecto)",
    "[610000]": "Cambio en Patrimonio",
    "[200000]": "Balance (Individual)",
    "[300000]": "EERR (Individual)",
    "[500000]": "EFE (Individual)",
    "[600000]": "Cambio en Patrimonio (Individual)"
}

# --- (V6.1) FILTRO DEL "OBRERO" (Whitelist) ---
LISTA_BLANCA_CODIGOS = [
    "[210000]", "[220000]", "[310000]", "[320000]", "[420000]", 
    "[510000]", "[520000]", "[200000]", "[300000]", "[500000]",
]

# --- ESTRATEGIA DE TRANSFORMACIÓN (Para la futura Capa Silver) ---
ESTRATEGIA_BALANCE = ["[210000]", "[220000]", "[200000]"]
ESTRATEGIA_EERR = ["[310000]", "[320000]", "[300000]"]
ESTRATEGIA_EFE = ["[510000]", "[520000]", "[500000]"]

# --- Parámetros de Scraper (Estables) ---
TRIMESTRES = ["03", "06", "09", "12"]
TIPO_NORMA = "IFRS" 
DELAY_NAVEGACION_CORTA = 1.2
DELAY_CONSULTA = 1.5
DELAY_ESPERA_CONSULTA = 4.0
URL_BUSQUEDA_CMF = "https://www.cmfchile.cl/portal/principal/613/w3-propertyname-815.html"