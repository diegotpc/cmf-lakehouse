import os
import boto3
from dotenv import load_dotenv

def purge_bucket():
    load_dotenv()

    # Validación de variables de entorno
    endpoint = os.getenv("R2_ENDPOINT_URL")
    access_key = os.getenv("R2_ACCESS_KEY")
    secret_key = os.getenv("R2_SECRET_KEY")

    if not all([endpoint, access_key, secret_key]):
        print("❌ Error: Faltan credenciales de R2 en el archivo .env")
        return

    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto'
    )

    bucket_name = "cmf-bronze"

    # --- SALVAGUARDA MANUAL ---
    print(f"\n⚠️  ADVERTENCIA CRÍTICA: Estás por vaciar el bucket '{bucket_name}'.")
    confirmacion = input(f"Escribe 'BORRAR' para confirmar la purga de la Capa Bronze: ")

    if confirmacion.strip().upper() != 'BORRAR':
        print("Abortando operación. No se eliminaron datos.")
        return

    # --- LÓGICA DE ELIMINACIÓN ---
    try:
        objetos = s3.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in objetos:
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objetos['Contents']]}
            s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)
            print(f"✅ Éxito: {len(objetos['Contents'])} objetos eliminados de {bucket_name}.")
        else:
            print(f"ℹ️ El bucket {bucket_name} ya se encuentra vacío.")
            
    except Exception as e:
        print(f"❌ Error durante la ejecución de purga: {str(e)}")

if __name__ == "__main__":
    purge_bucket()
