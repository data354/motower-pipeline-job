import logging
from minio import Minio
import pandas as pd
from pathlib import Path
import json
from io import BytesIO

config_file = Path(__file__).parents[3] / "config/configs.json"
# if db_file.exists():
#     with db_file.open("r",) as f:
#         settings = yaml.safe_load(f)
# else:
#     raise RuntimeError("database file don't exists")

if config_file.exists():
    with config_file.open("r",) as f:
        config = json.load(f)
else:
    raise RuntimeError("configs file don't exists")


def save_minio(endpoint, accesskey, secretkey, bucket: str, folder: str, date: str, data: pd.DataFrame) -> None:
    """
        save dataframe in minio
        Args:
            table [str]
            date [str]
            df [pd.DataFrame]
        Return
            None
    """
    client = Minio(
        endpoint,
        access_key= accesskey,
        secret_key= secretkey,
        secure=False)
    logging.info("start to save data")
    #objet = [t for t in config["tables"] if t["name"] == table][0]
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    csv_bytes = data.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)
    client.put_object(bucket,
                       f"{folder}/{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}.csv",
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')
    logging.info("data in minio ok")

#def read_minio(endpoint:str, accesskey, secretkey, buckect, folder)

def save_file_minio(endpoint:str, accesskey:str, secretkey:str,bucket:str, file:str):
    """
    save file into minio
    
    """
    client = Minio(
        endpoint,
        access_key= accesskey,
        secret_key= secretkey,
        secure=False)
    logging.info("start to save file")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    path = Path(__file__).parent / file
    client.fput_object(bucket, file, path)
