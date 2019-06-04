# churn_analysis

requirements:

* docker
* docker-compose
* python3

setup

```
docker-compose up -d
python3 -m venv churn_env
source churn_env/bin/activate
pip install db_utils
python3 seed_db.py
python3 churn_analysis.py
```