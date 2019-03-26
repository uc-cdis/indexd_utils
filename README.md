# indexd utils

Tools for indexing a list of objects in the provided manifest

### Setup

```
pip install -r requirement.txt
```

### How to run

```
python main.py indexing --prefix dg.123 --manifest /path/to/the/manifest.tsv
```

### Manifest formats
```
GUID md5	size	acl	url
ada53c3d-16ff-4e2a-8646-7cf78baf7aff	ff53a02d67fd28fcf5b8cd609cf51b06	137476	['phs000xxx']	s3://test_bucket/ada53c3d-16ff-4e2a-8646-7cf78baf7aff/test1.txt
2127dca2-e6b7-4e8a-859d-0d7093bd91e6	c4deccb43f5682cffe8f56c97d602d08	136553	['open']	s3://test_bucket/2127dca2-e6b7-4e8a-859d-0d7093bd91e6/test2.zip
```

