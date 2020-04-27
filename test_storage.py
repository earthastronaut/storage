#!python
import unittest

import storage


class Namespace:
    pass


config = Namespace()
config.DEBUG = True
config.MINIO_ENDPOINT = 'localhost:9000'
config.MINIO_ACCESS_KEY = 'ACCESSKEYWHICHSHOULDBECHANGED'
config.MINIO_SECRET_KEY = 'SECRETKEYWHICHSHOULDBECHANGED'


def create_client():
    return storage.StorageClient(
        endpoint=config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=(not config.DEBUG),
    )


class TestStorageClient(unittest.TestCase):
    testing_bucket = 'unittests'

    @classmethod
    def setUpClass(cls):
        cls.client = create_client()

    def setUp(self):
        self.client.get_or_make_bucket(self.testing_bucket)

    def tearDown(self):
        self.client.remove_bucket(self.testing_bucket, remove_objects=True)

    def test_storage_client(self):
        client = self.client

        obj_put = client.put_data(
            bucket_name=self.testing_bucket,
            object_name='turtle/rabbit.json',
            data={"hello": "world\u0000"},
            metadata={'meta': 'data'},
        )

        obj_get = client.get_storage_object(
            bucket_name=obj_put.bucket_name,
            object_name=obj_put.object_name,
        )

        self.assertEqual(obj_put, obj_get)

    def test_put_storage_object(self):
        client = self.client

        obj_put = client.create_storage_object(
            bucket_name=self.testing_bucket,
            object_name='turtle/rabbit.json',
            data={"hello": "world\u0000"},
            metadata={'meta': 'data'},
        )
        client.put_storage_object(obj_put)

        obj_get = client.get_storage_object(
            bucket_name=obj_put.bucket_name,
            object_name=obj_put.object_name,
        )

        self.assertEqual(obj_put, obj_get)

    def test_storage_client_serialize_metadata(self):
        client = self.client
        correct_answer = {'hello': '🌍'}
        answer = client.deserialize_metadata(
            client.serialize_metadata(correct_answer)
        )
        self.assertEqual(answer, correct_answer)

    def test_storage_client_serialize_data(self):
        client = self.client

        data = b'abc'
        answer = client.helper_serialize_data(data)
        correct_answer = {
            'data': b'abc',
            'data_length': 3,
            'content_type': 'application/octet-stream',
            'serializer_info': {
                'method': None,
                'encoding': None,
            }
        }
        self.assertEqual(answer, correct_answer)

        data = 'abc'
        answer = client.helper_serialize_data(data)
        correct_answer = {
            'data': b'abc',
            'data_length': 3,
            'content_type': 'application/octet-stream',
            'serializer_info': {
                'method': 'str',
                'encoding': 'utf-8',
            }
        }
        self.assertEqual(answer, correct_answer)

        data = 'abc'
        answer = client.helper_serialize_data(data, encoding='ascii')
        correct_answer = {
            'data': b'abc',
            'data_length': 3,
            'content_type': 'application/octet-stream',
            'serializer_info': {
                'method': 'str',
                'encoding': 'ascii',
            }
        }
        self.assertEqual(answer, correct_answer)

        data = {'hello': 'world'}
        answer = client.helper_serialize_data(data)
        correct_answer = {
            'data': b'{"hello": "world"}',
            'data_length': 18,
            'content_type': 'application/json; charset=utf-8',
            'serializer_info': {
                'method': 'json',
                'encoding': 'utf-8',
            }
        }
        self.assertEqual(answer, correct_answer)

        data = {'hello': 'world'}
        answer = client.helper_serialize_data(data, encoding='ascii')
        correct_answer = {
            'data': b'{"hello": "world"}',
            'data_length': 18,
            'content_type': 'application/json; charset=ascii',
            'serializer_info': {
                'method': 'json',
                'encoding': 'ascii',
            }
        }
        self.assertEqual(answer, correct_answer)

    def test_storage_client_remove_object(self):
        client = self.client

        obj = client.put_data(
            bucket_name=self.testing_bucket,
            object_name=f'rabbit_deleteme.json',
            data={"hello": "world"},
        )

        client.remove_storage_object(obj)

        self.assertRaises(
            storage.error.NoSuchKey,
            client.get_object,
            # **kws
            bucket_name=obj.bucket_name,
            object_name=obj.object_name,
        )

    def test_storage_client_remove_objects(self):
        client = self.client
        bucket_name = self.testing_bucket

        objects = []
        for i in range(3):
            objects.append(
                client.put_data(
                    bucket_name=bucket_name,
                    object_name=f'turtle/rabbit{i}.json',
                    data={"hello": i},
                )
            )
        client.remove_storage_objects(objects)

        for obj in objects:
            self.assertRaises(
                storage.error.NoSuchKey,
                client.get_object,
                # **kws
                bucket_name=obj.bucket_name,
                object_name=obj.object_name,
            )


if __name__ == '__main__':
    unittest.main()
