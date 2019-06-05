# churn_analysis

Churn analysis performed on e-commerce sample dataset.


requirements:
* docker
* docker-compose
* python3


setup:
```bash
docker-compose up -d
python3 -m venv churn_env
source churn_env/bin/activate
pip install db_utils
python3 churn_analysis.py
```
