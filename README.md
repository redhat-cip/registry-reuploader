# Usage

- Install python dependencies (cf. requirements.txt)

```
pip install -r requirements.txt
```

- Install & start Redis

```
yum install redis
systemctl enable --now redis
```

- Add your OpenStack credentials you the environment

```
source ~/openrc.sh
```

- Run the script, it will update its cache and check for missing blobs

```
python reupload_missing_files.py
```
