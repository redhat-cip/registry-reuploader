# Usage

- Fetch openstacksdk
- Apply this patch on openstacksdk: https://review.openstack.org/#/c/621381
- dnf install redis
- systemctl start redis
- python refresh_redis_cache.py
- python reupload_missing_files.py
