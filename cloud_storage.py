import os
import io
import logging
import boto3
from dotenv import load_dotenv

# Configurar el logger base
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def upload_to_r2(data_dict, rut, periodo):
    """
    Convierte un diccionario de DataFrames a CSV en memoria y los sube 
    a un bucket de R2.
    
    Args:
        data_dict (dict): Diccionario donde la llave es el 'codigo_xbrl' y el valor es el DataFrame.
        rut (str/int): RUT del emisor.
        periodo (str): Periodo de extracción.
        
    Returns:
        bool: True si todo se sube correctamente, False si ocurre algún error.
    """
    try:
        # Cargar credenciales desde .env
        load_dotenv()
        r2_access_key = os.getenv("R2_ACCESS_KEY")
        r2_secret_key = os.getenv("R2_SECRET_KEY")
        r2_endpoint_url = os.getenv("R2_ENDPOINT_URL")

        # Validar si existen todas las variables de entorno necesarias
        if not all([r2_access_key, r2_secret_key, r2_endpoint_url]):
            logger.error("Faltan variables de entorno para R2 (R2_ACCESS_KEY, R2_SECRET_KEY o R2_ENDPOINT_URL)")
            return False

        # Inicializar el cliente S3 para Cloudflare R2
        s3_client = boto3.client(
            service_name='s3',
            endpoint_url=r2_endpoint_url,
            aws_access_key_id=r2_access_key,
            aws_secret_access_key=r2_secret_key
        )
        
        bucket_name = "cmf-bronze"

        # Iterar el diccionario y subir cada DataFrame como CSV
        for codigo_xbrl, df in data_dict.items():
            # Convertir DataFrame a CSV crudo en memoria
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # Construir la llave del objeto (ruta del archivo en el bucket)
            object_key = f"rut={rut}/periodo={periodo}/{codigo_xbrl}.csv"
            
            # Subir a R2
            s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=csv_buffer.getvalue()
            )
            logger.info(f"Subida exitosa a R2: s3://{bucket_name}/{object_key}")
            
        return True

    except Exception as e:
        logger.error(f"Error técnico durante la subida a R2: {str(e)}")
        return False
