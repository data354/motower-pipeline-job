""" UTILS FUNCTION FOR MINIO"""
from typing import List
import logging
from io import BytesIO

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
    date_parts = date.split('-')
    if folder is not None:
        client.put_object(bucket,
                       f"{folder}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}.csv",
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')
        logging.info("data in minio ok")
    else:
        client.put_object(bucket,
                       f"{date_parts[0]}/{date_parts[1]}/{date_parts[2]}.csv",
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')
        logging.info("data in minio ok")

def get_latest_file(client, bucket: str, prefix: str = '', extensions: list = None):
    """
    Returns the name of the latest file in the S3 bucket with the specified prefix and extensions.
     :param client: an S3 client object
    :param bucket: the name of the S3 bucket
    :param prefix: the prefix of the file name (optional)
    :param extensions: a list of file extensions to search for (optional)
    :return: the name of the latest file with the specified prefix and extensions
    """
    extensions = ['.xlsx', '.xls', '.csv'] if extensions is None else extensions
    objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if not objects.get('Contents'):
        raise ValueError(f"No files found with prefix {prefix}")
    good_objects = [objet for objet in objects.get('Contents') if objet['Key'].endswith(tuple(extensions))]
    if not good_objects:
        raise ValueError(f"No files found with good extensions {extensions}")
    latest_file = max(good_objects, key=lambda x: x["LastModified"])
    return latest_file['Key']


def get_files(client, bucket: str, prefix: str = '', extensions: List[str] = None) -> List[str]:
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
    objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix, recursive = True)
    if not objects.get('Contents'):
        raise RuntimeError(f"No files found with prefix {prefix}")
    good_objects = [objet['Key'] for objet in objects.get('Contents') if objet['Key'].endswith(tuple(extensions))]
    if not good_objects:
        raise ValueError(f"No files found with good extensions {extensions}")
    return good_objects
