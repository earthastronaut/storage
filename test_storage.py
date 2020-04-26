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


class TestStorageClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = storage.StorageClient(
            endpoint=config.MINIO_ENDPOINT,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=(not config.DEBUG),
        )

    def setUp(self):
        self.bucket = 'rabbit'
        self.client.get_or_create_bucket(self.bucket)

    def test_storage_client(self):
        client = self.client

        obj_put = client.put_value(
            bucket=self.bucket,
            key='turtle/rabbit.json',
            value={"hello": "world\u0000"},
            metadata={'meta': 'data'},
        )

        obj_get = client.get_object(
            bucket=obj_put.bucket,
            key=obj_put.key,
        )

        self.assertEqual(obj_put, obj_get)

    def test_storage_client_serialize_metadata(self):
        client = self.client
        correct_answer = {'hello': '🌍'}
        answer = client.deserialize_metadata(
            client.serialize_metadata(correct_answer)
        )
        self.assertEqual(answer, correct_answer)

    def test_storage_client_serialize_value(self):
        client = self.client

        value = b'abc'
        answer = client.helper_serialize_value(value)
        correct_answer = {
            'value': b'abc',
            'value_length': 3,
            'content_type': 'application/octet-stream',
            'serializer_info': {
                'method': None,
                'encoding': None,
            }
        }
        self.assertEqual(answer, correct_answer)

        value = 'abc'
        answer = client.helper_serialize_value(value)
        correct_answer = {
            'value': b'abc',
            'value_length': 3,
            'content_type': 'application/octet-stream',
            'serializer_info': {
                'method': 'str',
                'encoding': 'utf-8',
            }
        }
        self.assertEqual(answer, correct_answer)

        value = 'abc'
        answer = client.helper_serialize_value(value, encoding='ascii')
        correct_answer = {
            'value': b'abc',
            'value_length': 3,
            'content_type': 'application/octet-stream',
            'serializer_info': {
                'method': 'str',
                'encoding': 'ascii',
            }
        }
        self.assertEqual(answer, correct_answer)

        value = {'hello': 'world'}
        answer = client.helper_serialize_value(value)
        correct_answer = {
            'value': b'{"hello": "world"}',
            'value_length': 18,
            'content_type': 'application/json; charset=utf-8',
            'serializer_info': {
                'method': 'json',
                'encoding': 'utf-8',
            }
        }
        self.assertEqual(answer, correct_answer)

        value = {'hello': 'world'}
        answer = client.helper_serialize_value(value, encoding='ascii')
        correct_answer = {
            'value': b'{"hello": "world"}',
            'value_length': 18,
            'content_type': 'application/json; charset=ascii',
            'serializer_info': {
                'method': 'json',
                'encoding': 'ascii',
            }
        }
        self.assertEqual(answer, correct_answer)

    def test_storage_client_remove_object(self):
        client = self.client

        obj = client.put_value(
            bucket=self.bucket,
            key=f'rabbit_deleteme.json',
            value={"hello": "world"},
        )

        client.remove_object(obj)

        self.assertRaises(
            storage.error.NoSuchKey,
            client.get_object,
            # **kws
            bucket=obj.bucket,
            key=obj.key,
        )

    def test_storage_client_remove_objects(self):
        client = self.client
        bucket = self.bucket

        objects = []
        for i in range(3):
            objects.append(
                client.put_value(
                    bucket=bucket,
                    key=f'turtle/rabbit{i}.json',
                    value={"hello": i},
                )
            )
        client.remove_objects(objects)

        for obj in objects:
            self.assertRaises(
                storage.error.NoSuchKey,
                client.get_object,
                # **kws
                bucket=obj.bucket,
                key=obj.key,
            )


if __name__ == '__main__':
    unittest.main()
