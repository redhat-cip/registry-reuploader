#!/bin.bash

import openstack
import redis
import json

redis_session = redis.Redis(host='localhost', port=6379, db=0)

# Initialize and turn on debug logging
openstack.enable_logging(debug=True)

# Initialize cloud
conn = openstack.connect()

print(conn.object_store)
container = conn.object_store.get_container_metadata('dci_registry')

#for o in conn.object_store.objects(container, prefix='files/docker/registry/v2/blobs/sha256'):
for o in conn.object_store.objects(container):
    if redis_session.exists(o.id):
        continue
    o = conn.object_store.get_object_metadata(o.name, container=container)
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
