""" UTILS FUNCTION FOR MINIO"""
from typing import List
import logging
from io import BytesIO
from pathlib import Path

def save_minio(client, bucket: str, folder: str, date: str, data) -> None:
    """
    save dataframe in minio
    Args:
        client: Minio client object
        bucket: Name of the bucket to save the file in
        folder: Folder path within the bucket to save the file in
        date: Date string in the format "YYYY-MM-DD"
        data: Pandas DataFrame object to save
    """
    logging.info("start to save data")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    csv_bytes = data.to_csv(index=False).encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)
    if folder is not None:
        client.put_object(bucket,
                       f"""{folder}/{date.split('-')[0]}/{date.split('-')[1]}/
                       {date.split('-')[2]}.csv""",
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')
        logging.info("data in minio ok")
    else:
        client.put_object(bucket,
                       f"{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}.csv",
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')
        logging.info("data in minio ok")

#def read_minio(endpoint:str, accesskey, secretkey, buckect, folder)

def save_file_minio(client, bucket:str, file:str):
    """
    save file into minio
    
    """
    logging.info("start to save file")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    path = Path(__file__).parent / file
    client.fput_object(bucket, file, path)


def get_last_filename(client,bucket:str, prefix:str = None,
                recursive:bool=True ):
    """
        get filename
    """
    objets = list(client.list_objects(bucket_name=bucket, prefix=prefix, recursive=recursive))
    if not objets:
        raise RuntimeError(f"file {prefix} don't exists")
    last_date = max([obj.last_modified for obj in objets])
    filename = [obj.object_name for obj in objets if obj.last_modified == last_date
                and obj.object_name.endswith([".xlsx", ".xls", "csv"])]
    if not filename:
        raise ValueError("file with good extension not found")
    return filename[0]



def get_files(client, bucket: str, prefix: str = None, extensions: List[str] = None) -> List[str]:
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
    objects = list(client.list_objects(bucket_name=bucket, prefix=prefix, recursive=True))
    if not objects:
        raise RuntimeError(f"No files found with prefix {prefix}")
    if extensions is None:
        extensions = []
    files = [obj.object_name for obj in objects
            if obj.object_name.lower().endswith(tuple(extensions))]
    if not files:
        raise ValueError(f"No files found with extensions {extensions}")
    return files
