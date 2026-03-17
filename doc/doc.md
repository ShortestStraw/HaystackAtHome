# DRAFT
## Haystack storage structure

Haystack idea is simple:

```
Volume file (Container for objects):
--------------------------------
|   Volume Magic - 8 bytes     | <-------------- Super block, that describes volume (e.g volume descriptor)
|   Volume Id - 8 bytes        |
|Volume maximal size - 8 bytes |
|------------------------------|
|   Needle 1                   |
|   Needle 2                   |
|   Needle 3                   |
............................
```

Each needle can be represented as:
```
Needle have to be aligned to 8 bytes
---------------------------
| Magic - 8 bytes         |
| KeySize - 4bytes        |
| Key - 1024 at max bytes |   <---- key size is questionable, we can change it (s3 standard used as hint)
| Padding - 4 bytes       |   <---- Can be used to implement versioning
| Flags - 8 bytes         |   <---- Can be NeedleTombstone representing that needle is deleted or outdated, 
| Size - 4 bytes          |   <---- Can be expanded, but we think that limiting object size to 4Gb is acceptable
| Checksum Type - 4 bytes |
| Data - 4Gb              |   <---- Yes, data is stored in needle
| Footer Magic - 8 bytes  |
| Checksum - 8 bytes      |   <---- expected to be crc64 or less
---------------------------
```

## Api:
    Use aws s3 api with signature v2 as example for implementation
    (Guess name for api will be kv-api or pitchfork)