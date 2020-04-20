#!python
import unittest

import storage


class Namespace:
    pass


config = Namespace()
config.DEBUG = True
config.MINIO_ENDPOINT = 'localhost:9000'
config.MINIO_ACCESS_KEY = 'AKIAIOSFODNN7EXAMPLE'
config.MINIO_SECRET_KEY = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'


class TestStorageClient(unittest.TestCase):

    def setUp(self):
        self.client = storage.StorageClient(
            endpoint=config.MINIO_ENDPOINT,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=(not config.DEBUG),
        )

    def test_storage_client(self):
        client = self.client

        obj_put = client.put_value(
            bucket='rabbit',
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


if __name__ == '__main__':
    unittest.main()
