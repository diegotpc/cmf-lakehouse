import os
import psycopg2
from dotenv import load_dotenv

# 1. Cargar configuración local
load_dotenv()
db_url = os.getenv("SUPABASE_DB_URL")

if not db_url:
    print("Error Crítico: Variable SUPABASE_DB_URL no detectada en el entorno.")
    exit(1)

conn = None
cursor = None

try:
    # 2. Inicializar conexión a la Capa de Control
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    # 3. Ejecutar consulta de validación
    query = "SELECT rut_emisor, periodo, estado FROM log_extraccion_cmf LIMIT 1;"
    cursor.execute(query)

    # 4. Extraer datos
    registro = cursor.fetchone()
    print(f"Estado: CONEXIÓN EXITOSA. Datos extraídos: {registro}")

except Exception as e:
    print(f"Estado: FALLO DE RED O AUTENTICACIÓN. Detalle técnico: {e}")

finally:
    # 5. Purga de recursos y cierre de conexión (Mandatorio)
    if cursor:
        cursor.close()
    if conn:
        conn.close()