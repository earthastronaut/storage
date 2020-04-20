# Storage

Wrapper around Minio with an interface for simple storage of python objects.

Problems this solves:

* File intermediaries. Can write bytes directly into the client
* Not true metadata. Minio metadata is written into headers which it decides keys making it hard to re-read. This serializes metadata and passes it in the headers. 
* No smart serialization. Provides helpers to serialize python objects and then deserialize them.


# Getting Started

Start up the minio service using docker-compose

```
docker-compose up storage
```

Then you can test in python with

```
import storage


client = storage.StorageClient(
    endpoint='localhost:9000',
    access_key='ACCESSKEYWHICHSHOULDBECHANGED',
    secret_key='SECRETKEYWHICHSHOULDBECHANGED',
    secure=False,
)

client.get_or_create_bucket('turtle')

obj_put = client.put_value(
    bucket='turtle',
    key='getting_started/example.json',
    value={'hello': 'world'},
    metadata={'meta': 'data'},
)

obj_get = client.get_object(
    bucket='turtle',
    key='getting_started/example.json',
)

obj_put.bucket = obj_get.bucket
obj_put.key == obj_get.key
obj_put.value == obj_get.value
obj_put.metadata == obj_get.metadata
obj_put == obj_get

```

