""" UTILS FUNCTION FOR MINIO"""

import logging
from io import BytesIO
from pathlib import Path



def save_minio(client, bucket: str, folder: str , date: str, data) -> None:
    """
        save dataframe in minio
        Args:
            table [str]
            date [str]
            df [pd.DataFrame]
        Return
            None
    """
    
    logging.info("start to save data")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    csv_bytes = data.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)
    if folder is not None:
        client.put_object(bucket,
                       f"{folder}/{date.split('-')[0]}/{date.split('-')[1]}/{date.split('-')[2]}.csv",
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


def getfilename(client,bucket:str, prefix:str = None,
                recursive:bool=True ):
    """
        get filename
    """
    
    objets = list(client.list_objects(bucket_name=bucket, prefix=prefix, recursive=recursive))
    if len(objets) == 0:
        raise RuntimeError(f"file {prefix} don't exists")
    
    last_date = max([obj.last_modified for obj in objets])
    
    filename = [obj.object_name for obj in objets if obj.last_modified == last_date][0]
    if (not filename.lower().endswith(".xlsx")) and (not filename.lower().endswith(".xls")) and (not filename.lower().endswith(".csv")):
        raise ValueError("file with good extension not found")
    return filename


def getfilesnames(client,bucket:str, prefix:str = None,
                recursive:bool=True):
    
    """
        get files names
    """
    
    objets = list(client.list_objects(bucket_name=bucket, prefix=prefix, recursive=recursive))
    if len(objets) == 0:
        raise RuntimeError(f"file {prefix} don't exists")
        
    filenames = [obj.object_name for obj in objets if obj.object_name.lower().endswith(".xls") or obj.object_name.endswith(".xlsx") or obj.object_name.endswith(".csv")  ]
    return filenames

