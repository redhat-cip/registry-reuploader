#!/bin/env python

import hashlib
import openstack
import openstack.exceptions
import redis
import requests
import json

redis_session = redis.Redis(host='localhost', port=6379, db=0)

# Initialize and turn on debug logging
openstack.enable_logging(debug=True)

# Initialize cloud
conn = openstack.connect()

container = conn.object_store.get_container_metadata('dci_registry')

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


def refresh_cache():
    for o in conn.object_store.objects(container):
        if redis_session.exists(o.id):
            continue
        try:
            o = conn.object_store.get_object_metadata(o.name, container=container)
        except openstack.exceptions.ResourceNotFound:
            print("Cannot stat %s on Swift" % o.name)
            continue
        print('o.id: %s' % o.id)
        data = {
                'content_length': o.content_length,
                'name': o.name,
                'id': o.id,
                'is_static_large_object': o.is_static_large_object,
                'copy_from': o.copy_from,
                'object_manifest': o.object_manifest,
                'multipart_manifest': o.multipart_manifest,
                'content_type': o.content_type,
                'last_modified_at': o.last_modified_at,
                }
        redis_session.set(o.id, json.dumps(data))


def reupload_blobs():
    for path, blob in empty_blobs():
        image = find_image(blob)
        print('Reuploading %s' % image)
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


refresh_cache()
reupload_blobs()
