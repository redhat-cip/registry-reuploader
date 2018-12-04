#!/usr/bin/python

import hashlib
import requests
import openstack
import redis
import json

#openstack.enable_logging(debug=True)
redis_session = redis.Redis(host='localhost', port=6379, db=0)
conn = openstack.connect()


def empty_blobs():
    for k in redis_session.keys():
        path = k.decode()
        v = redis_session.get(k)
        o = json.loads(v.decode())
        if not k.decode().endswith('/data'):
            continue
        if not path.startswith('files/docker/registry/v2/blobs/sha256'):
            continue
        if o['content_length']:
            continue
    
        yield (path, path.split('/')[7])

def find_image(blob):
    for k in redis_session.keys():
        path = k.decode()
        if not path.startswith('files/docker/registry/v2/repositories'):
            continue
        splitted = path.split('/')
        if not splitted[9] == blob:
            continue
        return "%s/%s" % (splitted[5], splitted[6])


for path, blob in empty_blobs():
    print(path)
    print(blob)
    image = find_image(blob)
    url = 'https://docker-registry.engineering.redhat.com/v2/%s/blobs/sha256:%s' % (image, blob)
    headers = {"Authorization": "Bearer anonymous"}
    r = requests.get(url, headers=headers, verify=False)
    digest = hashlib.sha256(r.content).hexdigest()
    if digest != blob:
        print('Invalid content!: %s %s' % (path, blob))
        continue

    ret = conn.object_store.upload_object(
            container="dci_registry",
            name=path,
            data=r.content)
    redis_session.delete(path)
