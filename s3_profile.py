"""
Módulo de Extracción y Perfilamiento de Datos desde AWS S3
----------------------------------------------------------
Descripción:
    Descarga datasets (CSV y JSON) desde un bucket de AWS S3
    utilizando un archivo de configuración centralizado, normaliza
    las estructuras y genera un perfilamiento estadístico básico.

Autor: Equipo de Ingeniería de Datos
Entorno: Python 3.14 (venv: proyecto001)
"""

import io
import json
import logging
import sys
import boto3
import pandas as pd
import yaml

# Configuración del sistema de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def cargar_configuracion(ruta_config: str = "config.yaml") -> dict:
    """Carga los parámetros y credenciales del archivo YAML.

    Args:
        ruta_config (str): Ruta al archivo de configuración.

    Returns:
        dict: Diccionario con la configuración cargada.
    """
    try:
        with open(ruta_config, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
            logging.info("Configuración cargada correctamente desde %s", ruta_config)
            return config
    except FileNotFoundError:
        logging.error("El archivo %s no fue encontrado.", ruta_config)
        sys.exit(1)
    except yaml.YAMLError as err:
        logging.error("Error al parsear el archivo YAML: %s", err)
        sys.exit(1)


def obtener_cliente_s3(config_aws: dict):
    """Inicializa y retorna el cliente de Boto3 para AWS S3.

    Args:
        config_aws (dict): Diccionario con parámetros de conexión AWS.

    Returns:
        boto3.client: Cliente Boto3 S3 configurado.
    """
    try:
        # Se verifica si existen credenciales explícitas en el config
        if "aws_access_key_id" in config_aws and config_aws["aws_access_key_id"]:
            return boto3.client(
                "s3",
                aws_access_key_id=config_aws.get("aws_access_key_id"),
                aws_secret_access_key=config_aws.get("aws_secret_access_key"),
                aws_session_token=config_aws.get("aws_session_token"),
                region_name=config_aws.get("region_name", "us-east-1"),
            )
        # Si no hay credenciales en config, se confía en las variables de entorno / AWS CLI
        return boto3.client("s3", region_name=config_aws.get("region_name", "us-east-1"))
    except Exception as err:
        logging.error("Error al inicializar el cliente de AWS S3: %s", err)
        raise err


def descargar_objeto_s3(s3_client, bucket_name: str, file_key: str) -> bytes:
    """Descarga el contenido binario de un objeto en AWS S3.

    Args:
        s3_client: Cliente de boto3 S3.
        bucket_name (str): Nombre del bucket S3.
        file_key (str): Ruta/clave del objeto en S3.

    Returns:
        bytes: Contenido binario del archivo descargado.
    """
    logging.info("Descargando objeto desde S3: s3://%s/%s", bucket_name, file_key)
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        return response["Body"].read()
    except Exception as err:
        logging.error("Error al descargar s3://%s/%s: %s", bucket_name, file_key, err)
        raise err


def convertir_bytes_a_dataframe(contenido_bytes: bytes, es_json: bool = False) -> pd.DataFrame:
    """Convierte un stream de bytes en un DataFrame de Pandas.

    Args:
        contenido_bytes (bytes): Datos en formato binario.
        es_json (bool): Indica si la fuente es un archivo JSON.

    Returns:
        pd.DataFrame: DataFrame construido a partir de los bytes.
    """
    if es_json:
        data_json = json.loads(contenido_bytes.decode("utf-8"))
        df = pd.json_normalize(data_json)
    else:
        df = pd.read_csv(io.BytesIO(contenido_bytes))
    return df


def perfilar_dataframe(df: pd.DataFrame, nombre_dataset: str) -> None:
    """Imprime en pantalla un resumen de perfilamiento de datos.

    Args:
        df (pd.DataFrame): Dataset a analizar.
        nombre_dataset (str): Etiqueta para identificar el dataset.
    """
    print(f"\n==================================================")
    print(f" PERFILAMIENTO DE DATASET: {nombre_dataset}")
    print(f"==================================================")
    print(f"Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas\n")
    
    print("--- Tipos de Datos ---")
    print(df.dtypes)
    
    print("\n--- Estadísticos Descriptivos (Cuantitativos) ---")
    print(df.describe().T)
    print("==================================================\n")


def cargar_y_perfilar_s3(s3_client, bucket_name: str, file_key: str, es_json: bool = False) -> pd.DataFrame:
    """Función orquestadora para la descarga, conversión y perfilamiento de un objeto S3.

    Args:
        s3_client: Cliente Boto3 S3.
        bucket_name (str): Nombre del bucket.
        file_key (str): Clave del archivo en S3.
        es_json (bool): True si el archivo es JSON, False si es CSV.

    Returns:
        pd.DataFrame: Dataset procesado y perfilado.
    """
    contenido_bytes = descargar_objeto_s3(s3_client, bucket_name, file_key)
    df = convertir_bytes_a_dataframe(contenido_bytes, es_json=es_json)
    perfilar_dataframe(df, file_key)
    return df


def main():
    """Ejecución principal del pipeline de perfilamiento."""
    logging.info("=== INICIANDO PIPELINE DE PERFILAMIENTO S3 ===")
    
    # 1. Cargar archivo de configuración
    config = cargar_configuracion("config.yaml")
    config_aws = config.get("aws", {})
    bucket_name = config_aws.get("bucket_name", "aws-academy-datacenter-bucket-22-03-008")

    # 2. Inicializar cliente S3
    s3_client = obtener_cliente_s3(config_aws)

    # 3. Descargar y perfilar datasets de prueba
    try:
        df_transacciones = cargar_y_perfilar_s3(
            s3_client, bucket_name, "raw/transacciones.csv", es_json=False
        )
        df_logs = cargar_y_perfilar_s3(
            s3_client, bucket_name, "raw/logs_m2m.json", es_json=True
        )
        logging.info("=== PROCESO DE PERFILAMIENTO COMPLETADO CON ÉXITO ===")
    except Exception as err:
        logging.error("Fallo crítico en el pipeline: %s", err)


if __name__ == "__main__":
    main()