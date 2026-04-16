import sys
try:
    import selenium
    import bs4
    import boto3
    import psycopg2
    import duckdb
    import dotenv
    print("Estado: APROBADO. Todas las dependencias base están instaladas.")
    print(f"Ruta del ejecutable: {sys.executable}")
except ImportError as e:
    print(f"Estado: ERROR. Falta la dependencia: {e}")