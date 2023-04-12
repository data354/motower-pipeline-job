import logging
from io import BytesIO
from pathlib import Path
from minio import Minio
import pandas as pd



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
    #objet = [t for t in CONFIG["tables"] if t["name"] == table][0]
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

def getfilename(endpoint:str, accesskey:str, secretkey:str,bucket:str, prefix:str = None,
                recursive:bool=True ):
    """
        get filename
    """
    client = Minio(
        endpoint,
        access_key= accesskey,
        secret_key= secretkey,
        secure=False)
    objets = list(client.list_objects(bucket_name=bucket, prefix=prefix, recursive=recursive))
    if len(objets) == 0:
        raise RuntimeError(f"file {prefix} don't exists")
    
    last_date = max([obj.last_modified for obj in objets])
    
    filename = [obj.object_name for obj in objets if obj.last_modified == last_date][0]
    if (not filename.lower().endswith(".xlsx")) and (not filename.lower().endswith(".xls")) and (not filename.lower().endswith(".csv")):
        raise ValueError("file with good extension not found")
    return filename


def getfilesnames(endpoint:str, accesskey:str, secretkey:str,bucket:str, prefix:str = None,
                recursive:bool=True):
    
    """
        get files names
    """
    client = Minio(
        endpoint,
        access_key= accesskey,
        secret_key= secretkey,
        secure=False)
    objets = list(client.list_objects(bucket_name=bucket, prefix=prefix, recursive=recursive))
    if len(objets) == 0:
        raise RuntimeError(f"file {prefix} don't exists")
        
    filenames = [obj.object_name for obj in objets if obj.object_name.lower().endswith(".xls") or obj.object_name.endswith(".xlsx") or obj.object_name.endswith(".csv")  ]
    return filenames
# def read_minio(endpoint, accesskey, secretkey, bucket: str, folder:str, prefix:str, path: str, date: str)-> pd.DataFrame:
#     """
#         read data into minio
#     """
#     client = Minio(
#         endpoint,
#         access_key= accesskey,
#         secret_key= secretkey,
#         secure=False)
    
#     if not client.bucket_exists(bucket):
#         raise OSError(f"bucket {bucket} don't exists")
#     try:
        
#         file = client.get_object(bucket, path)
#     except ResponseError as err:
#         raise OSError(f"file {path} don't exists in minio") from err
    
#     return data