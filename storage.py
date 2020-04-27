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


__version__ = '2.0.0'


class StorageObject:

    def __init__(
        self,
        bucket_name,
        object_name,
        data,
        metadata=None,
    ):
        self.__dict__ = {
            'bucket_name': bucket_name,
            'object_name': object_name,
            'data': data,
            'metadata': metadata or {}
        }

    def as_dict(self):
        return self.__dict__

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class StorageClient(Minio):
    """ Wrapper around Minio client which provides byte object put/get

    Args:
        endpoint (str): Hostname of the cloud storage server.
        access_key (str): Access key to sign self._http.request with.
        secret_key (str): Secret key to sign self._http.request with.
        session_token (str): Session token to sign self._http.request with.
        secure (bool): Set this value if wish to make secure requests. Default is True.
        region (str): Set this value to override automatic bucket location discovery.
        timeout (int): Set this value to control how long requests

    """

    StorageObjectClass = StorageObject

    def close(self):
        self._http.clear()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def get_or_make_bucket(self, bucket_name):
        try:
            self.make_bucket(bucket_name)
        except (
            error.BucketAlreadyOwnedByYou,
            error.BucketAlreadyExists,
        ):
            pass
        return bucket_name

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
    def helper_serialize_data(data, encoding='utf-8'):
        if isinstance(data, bytes):
            serializer_method = None
            encoding = None
            content_type = 'application/octet-stream'
            data_serialized = data

        elif isinstance(data, str):
            serializer_method = 'str'
            encoding = encoding
            content_type = 'application/octet-stream'
            data_serialized = data.encode(encoding)

        elif isinstance(data, dict):
            serializer_method = 'json'
            encoding = encoding
            content_type = f'application/json; charset={encoding}'
            data_serialized = json.dumps(data).encode(encoding)

        else:
            raise TypeError(
                f'No method for converting type {type(data)} to bytes'
            )

        return {
            'data': data_serialized,
            'data_length': len(data_serialized),
            'content_type': content_type,
            'serializer_info': {
                'method': serializer_method,
                'encoding': encoding,
            }
        }

    @staticmethod
    def helper_deserialize_data(data, serializer_info=None):
        if serializer_info is None:
            return data

        serializer_method = serializer_info['method']
        encoding = serializer_info['encoding']
        if serializer_method is None:
            return data

        elif serializer_method == 'str':
            return data.decode(encoding)

        elif serializer_method == 'json':
            return json.loads(data.decode(encoding))

        else:
            raise error.MinioError(f'Unknown serializer method {serializer_method}')  # noqa

    def create_storage_object(self, bucket_name, object_name, data=None, metadata=None, **kws):  # noqa
        return self.StorageObjectClass(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
            metadata=metadata,
            **kws
        )

    def put_data(self, bucket_name, object_name, data, metadata=None, encoding='utf-8'):
        """Put the content into storage.

        Args:
            bucket_name (str): Bucket category for the data.
            object_name (str): Key to store data at.
            data (bytes | str | dict): Storage objects. Has multiple options.
                If other than bytes are provided then metadata will be updated.

                * bytes: store these bytes exactly
                * str: will use the "encoding" parameter to encode.
                    metadata['encoding'] = encoding
                * dict: will use json.dumps(data).encode('utf-8')
                    metadata.setdefault(
                        'content_type', 'application/json; charset=utf-8')
            metadata (dict): Information store about the data.
            encoding (str): Used for encoding if str is provided.

        Returns:
            StorageObject: new storage object from the components including the
                storage client.
        """
        storage_object = self.create_storage_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
            metadata=metadata,
        )

        serialized = self.helper_serialize_data(data, encoding=encoding)

        super().put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=io.BytesIO(serialized['data']),
            content_type=serialized['content_type'],
            length=serialized['data_length'],
            metadata={
                # these are headers which minio transforms metadata into
                'X-Amz-Meta-SerializerInfo': self.serialize_metadata(serialized['serializer_info']),  # noqa
                'X-Amz-Meta-Metadata': self.serialize_metadata(metadata),
            },
        )
        return storage_object

    def put_storage_object(self, storage_object, encoding='utf-8'):
        """Put object into storage.

        Also see: help(put_data)

        Args:
            storage_object (StorageObject): The object containing the bucket
                key, data, and metadata.
            encoding (str): Used for encoding if str is provided.

        Returns:
            StorageObject: new storage object from the components including the
                storage client.
        """
        self.put_data(
            bucket_name=storage_object.bucket_name,
            object_name=storage_object.object_name,
            data=storage_object.data,
            metadata=storage_object.metadata,
            encoding=encoding,
        )

    def get_storage_object(self, bucket_name, object_name, deserialize=True):
        """Get object and metadata from storage

        Args:
            bucket_name (str): bucket_name category for the data.
            object_name (str): Key to store data at.
            deserialize (bool): If True then will attempt to deserialize data.

        Returns:
            StorageObject: The object sent to storage.

        """
        response = super().get_object(
            bucket_name=bucket_name,
            object_name=object_name,
        )

        data = self.helper_deserialize_data(
            response.read(),
            serializer_info=self.deserialize_metadata(
                response.getheader('X-Amz-Meta-SerializerInfo')
            )
        )
        metadata = self.deserialize_metadata(
            response.getheader('X-Amz-Meta-Metadata')
        )

        return self.create_storage_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
            metadata=metadata,
        )

    def remove_storage_object(self, storage_object):
        """ Remove object from bucket.

        Args:
            storage_object (StorageObject): The bucket and object_name of the 
                storage object.
        """
        super().remove_object(
            bucket_name=storage_object.bucket_name,
            object_name=storage_object.object_name,
        )

    def remove_storage_objects(self, storage_objects):
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
            objects_by_bucket.setdefault(obj.bucket_name, []).append(obj.object_name)  # noqa

        for bucket_name, object_names in objects_by_bucket.items():
            errors = list(super().remove_objects(
                bucket_name=bucket_name,
                objects_iter=object_names,
            ))

            if len(errors):
                error_msg = '\n\n'.join(map(str, errors))
                raise error.MinioError(error_msg)

    def remove_bucket(self, bucket_name, remove_objects=False):
        """ Remove a bucket.

        Args:
            bucket_name (str): Name of the bucket.
            remove_objects (bool): If True then remove all objects in the
                bucket first.
        """
        if remove_objects:
            list_objects = self.list_objects_v2(bucket_name, recursive=True)
            self.remove_storage_objects(list_objects)
        super().remove_bucket(bucket_name)
