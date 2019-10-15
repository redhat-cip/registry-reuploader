#!/bin/env python

import argparse
import fcntl
import hashlib
import json
import openstack
import openstack.exceptions
import redis
import requests
import sys


pid_file = '/tmp/reupload_missing_files.lock'
fp = open(pid_file, 'w')
try:
    fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
except IOError:
    sys.stderr.write("Another instance of this program is already running\n")
    sys.exit(1)


parser = argparse.ArgumentParser(description='Search for missing container '
                                 'images blobs in swift and upload them')
parser.add_argument('--debug', action='store_true')
args = parser.parse_args()

redis_session = redis.Redis(host='localhost', port=6379, db=0)

# Initialize and turn on debug logging
openstack.enable_logging(debug=args.debug)

# Initialize cloud
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


def refresh_cache():
    container = conn.object_store.get_container_metadata('dci_registry')

    for o in conn.object_store.objects(container):
        if redis_session.exists(o.id):
            continue
        try:
            o = conn.object_store.get_object_metadata(o.name,
                                                      container=container)
        except openstack.exceptions.ResourceNotFound:
            print("Cannot stat %s on Swift" % o.name)
            continue
        #print('o.id: %s' % o.id)
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

def get_blob_content_v1(image, blob):
    print("Trying to get the blob from docker-registry.engineering.redhat.com ...")
    url = 'https://docker-registry.engineering.redhat.com/v2/%s/blobs/sha256:%s' % (image, blob) # noqa
    headers = {"Authorization": "Bearer anonymous"}
    r = requests.get(url, headers=headers, verify=False)
    return r


def get_blob_content_v2(image, blob):
    print("Trying to get the blob from registry-proxy.engineering.redhat.com ...")
    url = "https://registry-proxy.engineering.redhat.com"
    ns = "rh-osbs"
    image = image.replace("/", "-")

    token = requests.get(
        "%s/v2/auth?scope=repository:%s/%s:pull" % (url, ns, image), verify=False).json()["token"]
    headers = {"Authorization": "Bearer %s" % token}

    r = requests.get("%s/v2/%s/%s/blobs/sha256:%s" % (url, ns, image, blob), headers=headers, verify=False)
    return r

def reupload_blobs():
    for path, blob in empty_blobs():
        image = find_image(blob)
        print('Trying to reupload %s ...' % image)
        r = get_blob_content_v1(image, blob)
        digest = hashlib.sha256(r.content).hexdigest()
        if digest != blob:
            print("Bad response, moving on. HTTP STATUS %d" % r.status_code)
            r = get_blob_content_v2(image, blob)

            digest = hashlib.sha256(r.content).hexdigest()
            if digest != blob:
                print("Bad response, moving on. HTTP STATUS %d" % r.status_code)
                continue

        print("Success. Now pushing the blob to Swift ...")
        conn.object_store.upload_object(container="dci_registry", name=path, data=r.content)
        redis_session.delete(path)
        print("Done.")

if __name__ == "__main__":
    refresh_cache()
    reupload_blobs()
