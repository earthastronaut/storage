import base64
import io
import json

from minio import (
    error,
    Minio,
)

__all__ = [
    'error',
    'StorageObject',
    'StorageClient',
]


__version__ = '1.0.0'


class StorageObject:

    def __init__(
        self,
        bucket,
        key,
        value,
        metadata=None,
    ):
        self.__dict__ = {
            'bucket': bucket,
            'key': key,
            'value': value,
            'metadata': metadata or {}
        }

    def as_dict(self):
        return self.__dict__

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class StorageClient:
    """ Wrapper around Minio client which provides byte object put/get

    Args:
        endpoint (str): Hostname of the cloud storage server.
        access_key (str): Access key to sign self._http.request with.
        secret_key (str): Secret key to sign self._http.request with.
        session_token (str): Session token to sign self._http.request with.
        secure (bool): Set this value if wish to make secure requests. Default is True.
        region (str): Set this value to override automatic bucket location discovery.
        timeout (int): Set this value to control how long requests

    Example:

    """

    StorageObjectClass = StorageObject

    def __init__(self, endpoint, **kws):
        kws['endpoint'] = endpoint
        self.minio_client = Minio(**kws)

    def get_or_create_bucket(self, bucket):
        try:
            self.minio_client.make_bucket(bucket)
        except (
            error.BucketAlreadyOwnedByYou,
            error.BucketAlreadyExists,
        ):
            pass
        return bucket

    @staticmethod
    def serialize_metadata(metadata):
        """ Returns a string to be passed by requests headers through minio """
        if metadata is None:
            return ''
        s = json.dumps(metadata)
        s = s.encode('utf-8')
        s = base64.b64encode(s)
        return s.decode('ascii')

    @staticmethod
    def deserialize_metadata(metadata_serialized):
        """ Takes serialized metadata string and returns dict """
        if metadata_serialized is None or metadata_serialized == '':
            return None
        s = metadata_serialized.encode('ascii')
        s = base64.b64decode(s)
        s = s.decode('utf-8')
        return json.loads(s)

    @staticmethod
    def helper_serialize_value(value, encoding='utf-8'):
        if isinstance(value, bytes):
            serializer_method = None
            encoding = None
            content_type = 'application/octet-stream'
            value_serialized = value

        elif isinstance(value, str):
            serializer_method = 'str'
            encoding = encoding
            content_type = 'application/octet-stream'
            value_serialized = value.encode(encoding)

        elif isinstance(value, dict):
            serializer_method = 'json'
            encoding = encoding
            content_type = f'application/json; charset={encoding}'
            value_serialized = json.dumps(value).encode(encoding)

        else:
            raise TypeError(
                f'No method for converting type {type(value)} to bytes'
            )

        return {
            'value': value_serialized,
            'value_length': len(value_serialized),
            'content_type': content_type,
            'serializer_info': {
                'method': serializer_method,
                'encoding': encoding,
            }
        }

    @staticmethod
    def helper_deserialize_value(value, serializer_info=None):
        if serializer_info is None:
            return value

        serializer_method = serializer_info['method']
        encoding = serializer_info['encoding']
        if serializer_method is None:
            return value

        elif serializer_method == 'str':
            return value.decode(encoding)

        elif serializer_method == 'json':
            return json.loads(value.decode(encoding))

        else:
            raise error.MinioError(f'Unknown serializer method {serializer_method}')  # noqa

    def create_storage_object(self, bucket, key, value=None, metadata=None, **kws):  # noqa
        return self.StorageObjectClass(
            bucket=bucket,
            key=key,
            value=value,
            metadata=metadata,
            **kws
        )

    def put_value(self, bucket, key, value, metadata=None, encoding='utf-8'):
        """Put the content into storage.

        Args:
            bucket (str): Bucket category for the data.
            key (str): Key to store data at.
            value (bytes | str | dict): Storage objects. Has multiple options.
                If other than bytes are provided then metadata will be updated.

                * bytes: store these bytes exactly
                * str: will use the "encoding" parameter to encode.
                    metadata['encoding'] = encoding
                * dict: will use json.dumps(value).encode('utf-8')
                    metadata.setdefault(
                        'content_type', 'application/json; charset=utf-8')
            metadata (dict): Information store about the data.
            encoding (str): Used for encoding if str is provided.

        Returns:
            StorageObject: new storage object from the components including the
                storage client.
        """
        storage_object = self.create_storage_object(
            bucket=bucket,
            key=key,
            value=value,
            metadata=metadata,
        )

        serialized = self.helper_serialize_value(value, encoding=encoding)

        self.minio_client.put_object(
            bucket_name=bucket,
            object_name=key,
            data=io.BytesIO(serialized['value']),
            content_type=serialized['content_type'],
            length=serialized['value_length'],
            metadata={
                # these are headers which minio transforms metadata into
                'X-Amz-Meta-SerializerInfo': self.serialize_metadata(serialized['serializer_info']),  # noqa
                'X-Amz-Meta-Metadata': self.serialize_metadata(metadata),
            },
        )
        return storage_object

    def put_object(self, storage_object, encoding='utf-8'):
        """Put object into storage.

        Also see: help(put_value)

        Args:
            storage_object (StorageObject): The object containing the bucket
                key, value, and metadata.
            encoding (str): Used for encoding if str is provided.

        Returns:
            StorageObject: new storage object from the components including the
                storage client.
        """
        self.put_value(
            bucket=storage_object.bucket,
            key=storage_object.key,
            value=storage_object.value,
            metadata=storage_object.metadata,
            encoding=encoding,
        )

    def get_object(self, bucket, key, deserialize=True):
        """Get object and metadata from storage

        Args:
            bucket (str): Bucket category for the data.
            key (str): Key to store data at.
            deserialize (bool): If True then will attempt to deserialize value.

        Returns:
            StorageObject: The object sent to storage.

        """
        response = self.minio_client.get_object(
            bucket_name=bucket,
            object_name=key,
        )

        value = self.helper_deserialize_value(
            response.read(),
            serializer_info=self.deserialize_metadata(
                response.getheader('X-Amz-Meta-SerializerInfo')
            )
        )
        metadata = self.deserialize_metadata(
            response.getheader('X-Amz-Meta-Metadata')
        )

        return StorageObject(
            bucket=bucket,
            key=key,
            value=value,
            metadata=metadata,
        )

    def remove_object(self, storage_object):
        """ Remove object from bucket.

        Args:
            storage_object (StorageObject): The bucket and key of the storage
                object.
        """
        self.minio_client.remove_object(
            bucket_name=storage_object.bucket,
            object_name=storage_object.key,
        )

    def remove_objects(self, storage_objects):
        """ Remove objects from bucket.

        Objects are deleted in batch. 

        Args:
            storage_objects (List[StorageObject]): List-like iterator of 
                storage objects.

        Raises:
            RemoveObjectError: If any errors are returned for deleting objects.
        """
        objects_by_bucket = {}
        for obj in storage_objects:
            objects_by_bucket.setdefault(obj.bucket, []).append(obj.key)

        for bucket, keys in objects_by_bucket.items():
            errors = list(self.minio_client.remove_objects(
                bucket_name=bucket,
                objects_iter=keys,
            ))

            if len(errors):
                error_msg = '\n\n'.join(map(str, errors))
                raise error.MinioError(error_msg)
