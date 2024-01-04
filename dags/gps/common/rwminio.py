""" UTILS FUNCTION FOR MINIO"""
from typing import List
import logging
from io import BytesIO
import pandas as pd
from minio import Minio

def save_minio(client: Minio, bucket: str, date: str, data, folder=None) -> None:
    """
    save dataframe in minio
    Args:
        client: Minio client object
        bucket: Name of the bucket to save the file in
        folder: Folder path within the bucket to save the file in
        date: Date string in the format "YYYY-MM-DD"
        data: Pandas DataFrame object to save
    """
    try:
        logging.info("start to save data")
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        
        csv_bytes = data.to_csv(index=False).encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)
        date_parts = date.split('-')
        object_path = f"{folder}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}.csv" if folder else f"{date_parts[0]}/{date_parts[1]}/{date_parts[2]}.csv"
        
        client.put_object(bucket,
                          object_path,
                          data=csv_buffer,
                          length=len(csv_bytes),
                          content_type='application/csv')
        
        logging.info("data in minio ok")
    except Exception as e:
        logging.error("Error saving data to Minio with error: %s", str(e))
        

def get_latest_file(client: Minio, bucket: str, prefix: str = '', extensions: list = None):
    """
    Returns the name of the latest file in the S3 bucket with the specified prefix and extensions.
    :param client: an S3 client object
    :param bucket: the name of the S3 bucket
    :param prefix: the prefix of the file name (optional)
    :param extensions: a list of file extensions to search for (optional)
    :return: the name of the latest file with the specified prefix and extensions
    """
    extensions = ['.xlsx', '.xls', '.csv'] if extensions is None else extensions
    try:
        objects = client.list_objects(bucket_name=bucket, prefix=prefix, recursive=True)
    except Exception as e:
        logging.error("Error getting files with error %s", str(e))

    if not objects:
        return None
    good_objects = [obj for obj in objects if obj.object_name.lower().endswith(tuple(extensions))]
    if not good_objects:
        return None
    latest_file = max(good_objects, key=lambda x: x.last_modified)
    return latest_file.object_name



def read_file(client: Minio, bucket_name: str, object_name: str, sheet_name = '', header_num:int=0, sep:str=';') -> pd.DataFrame:
    """
    
        read file from minio
    """
    try:
        rep = client.get_object(bucket_name=bucket_name, object_name=object_name)
        data = BytesIO()
        chunk_size = 1024  # Adjust the chunk size as per your requirements
        
        while True:
            chunk = rep.read(chunk_size)
            if not chunk:
                break
            data.write(chunk)
        
        data.seek(0)
        object_name_lower = object_name.lower()
        
        if object_name_lower.endswith(".csv"):
            df = pd.read_csv(data, sep=sep)
        elif object_name_lower.endswith((".xlsx", "xls")):
            if sheet_name == '':
                df = pd.read_excel(data, header=header_num)
            else:
                df = pd.read_excel(data, header=header_num, sheet_name=sheet_name)
                
        return df
    
    except Exception as e:
        logging.error("Error reading data to Minio with error: %s", str(e))
    finally:
        rep.close()
        rep.release_conn()



def get_files(client: Minio, bucket: str, prefix: str = '', extensions: List[str] = None) -> List[str]:
    """
    Get a list of names of all files in the bucket that match the given prefix and extensions.
    Args:
        client: Minio client object
        bucket: Name of the bucket to search in
        prefix: Prefix to filter the files by
        extensions: List of extensions to filter the files by
    Returns:
        List of names of all files that match the given criteria
    """
    extensions = ['.xlsx', '.xls', '.csv'] if extensions is None else extensions
    extensions = [ext.lower() for ext in extensions]
    objects = client.list_objects(bucket_name=bucket, prefix=prefix, recursive = True)
    if not objects:
        raise RuntimeError(f"No files found with prefix {prefix}")
    good_objects = [obj.object_name for obj in objects if obj.object_name.lower().endswith(tuple(extensions))]
    if not good_objects:
        raise ValueError(f"No files found with good extensions {extensions}")
    return good_objects




