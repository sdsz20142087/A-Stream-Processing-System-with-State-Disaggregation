datanames = [
    {
        'name': 'local1.txt',
        "ref": "source=12, count=8, replicas=1, localkv, junk=1",
        'colName': 'localKV1',
    },
    {
        'name': 'local2.txt',
        "ref": "source=12, count=20, replicas=1, localkv, junk=1",
        'colName': 'localKV2',
    },
    {
        'name': 'local3.txt',
        "ref":"source=15, count=20, replicas=2, localkv, junk=1, migrate=1682491160567",
        "migrate": "1682491160567",
        'colName': 'localKV3',
    },
    {
        'name': 'hybrid1.txt',
        "ref": "source=14, count=12, replicas=1, hybridkv, junk=5",
        "colName": "hybridKV1",
    },
    {
        "name": "hybrid2.txt",
        "ref": "source=9, count=12, replicas=2, hybridkv, junk=5, cache=no",
        "colName": "hybridKV2",
    },
    {
        "name": "hybrid3.txt",
        "ref": "source=9, count=12, replicas=2, hybridkv, junk=5, cache=yes",
        "colName": "hybridKV3",
    },
    {
        "name": "hybrid4.txt",
        "ref": "source=11, count=12, replicas=2, hybridkv, junk=5, cache=yes, boot=1682536203097",
        "migrate": "1682536203097",
        "colName": "hybridKV4",
    },
    {
        "name": "hybrid5.txt",
        "ref": "source=11, count=12, replicas=2, hybridkv, junk=5, cache=yes, migrate=yes, boot=1682546002468",
        "migrate": "1682546002468",
        "colName": "hybridKV5",
    }
]