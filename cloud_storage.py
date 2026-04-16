import os
import io
import csv
import logging
import boto3
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def upload_to_r2(datos_totales, rut, periodo):
    """
    Subida Agnóstica Bronze 2.0. Exporta cada Key del diccionario de reportes
    en su propio archivo local .CSV sobre la capa de objetos distribuidos usando R2.
    El dict usa llaves como 'C_210000' o 'I_810000'.
    
    Args:
        datos_totales (dict): El master dict de Consolidados e Individuales de un RUT/Periodo.
        rut (str/int): RUT del emisor base.
        periodo (str): Cadena textual de la ventana temporal.
        
    Returns:
        bool: Control de status operacional.
    """
    try:
        load_dotenv()
        r2_access_key = os.getenv("R2_ACCESS_KEY")
        r2_secret_key = os.getenv("R2_SECRET_KEY")
        r2_endpoint_url = os.getenv("R2_ENDPOINT_URL")

        if not all([r2_access_key, r2_secret_key, r2_endpoint_url]):
            logger.error("Error crítico: Credenciales AWS/R2 huérfanas en el entorno (.env).")
            return False

        s3_client = boto3.client(
            service_name='s3',
            endpoint_url=r2_endpoint_url,
            aws_access_key_id=r2_access_key,
            aws_secret_access_key=r2_secret_key
        )
        
        bucket_name = "cmf-bronze"

        for tipo_codigo, doc_obj in datos_totales.items():
            csv_buffer = io.StringIO()
            # La orden es respetar delimitador por `;` y dejar valores agnósticos
            writer = csv.writer(csv_buffer, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
            headers = doc_obj.get("headers", [])
            if headers:
                writer.writerow(headers)
                
            data_rows = doc_obj.get("data", [])
            for row in data_rows:
                writer.writerow(row)
            
            # File key estandarizado
            object_key = f"rut={rut}/periodo={periodo}/{tipo_codigo}.csv"
            
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=csv_buffer.getvalue()
            )
            logger.info(f"☁️ Fragmento guardado en Bronze (R2): s3://{bucket_name}/{object_key}")
            
        return True

    except Exception as e:
        logger.error(f"Falla fatal enviando I/O sobre R2: {str(e)}")
        return False
