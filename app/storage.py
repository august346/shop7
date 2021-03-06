import io
import os

from minio import Minio, S3Error


class Bucket:
    files = 'files'
    reports = 'reports'

    @classmethod
    def get_bucket_names(cls):
        yield cls.files
        yield cls.reports


client = Minio(
    os.environ.get('MINIO_URL', 'localhost:9000'),
    access_key=os.environ['MINIO_ACCESS_KEY'],
    secret_key=os.environ['MINIO_SECRET_KEY'],
    secure=False
)

for bucket_name in Bucket.get_bucket_names():
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def save(bucket: Bucket, name: str, file: io.BytesIO) -> None:
    size = file.getbuffer().nbytes

    client.put_object(
        bucket_name=bucket,
        object_name=name,
        data=file,
        length=size,
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


def get(bucket: Bucket, file_id: str):
    try:
        return client.get_object(bucket, file_id)
    except S3Error:
        return None
