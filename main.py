import json
import logging

import requests
from databricks import sql


def generate_json(name: str, existing_cluster_id: str):
    """
    Create json template to use with Jobs API
    :return:
    """
    return {
        "name": f'{name}_model_training',
        "tasks": [
            {
                "task_key": "maitreyi-test",
                "spark_python_task": {
                    "python_file": "/Repos/mrajaram@integralads.com/databricks-test/run_model.py"
                },
                "existing_cluster_id": existing_cluster_id,
                "libraries": [
                    {
                        "pypi": {
                            "package": "databricks-sql-connector==2.3.0"
                        }
                    }
                ],
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "auto_scaling_cluster",
                "new_cluster": {
                    "spark_version": "11.3.x-scala2.12",
                    "node_type_id": "m6g.xlarge",
                    "spark_conf": {},
                    "aws_attributes": {
                        "first_on_demand": 1,
                        "availability": "SPOT",
                        "zone_id": "auto",
                        "instance_profile_arn": "arn:aws:iam::805967804917:instance-profile/GlueCatalogIntegration",
                        "spot_bid_price_percent": 75,
                        "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                        "ebs_volume_count": 3,
                        "ebs_volume_size": 100
                    },
                    "autoscale": {
                        "min_workers": 1,
                        "max_workers": 2
                    }
                }
            }
        ],
        "timeout_seconds": 86400,
        "schedule": {
            "quartz_cron_expression": "0 0 0 * * ?",
            "timezone_id": "Etc/UTC"
        },
        "max_concurrent_runs": 10,
        "format": "MULTI_TASK"
    }


def create_job(endpoint: str, token: str, name: str, cluster_id: str):
    headers = {"Authorization": f'Bearer {token}'}
    r = requests.post(endpoint, headers=headers, json=generate_json(name=name, existing_cluster_id=cluster_id))
    if r.status_code == 200:
        logging.info(r['job_id'])
    else:
        logging.error(r.text)


def main():
    """
    Testing dynamic workflows on Databricks
    :return:
    """
    config = json.loads(open("config.json").read())
    token = dbutils.secrets.get(scope="workflow", key="databricks_token")
    env = config.get("env")
    with sql.connect(server_hostname=f'ias-{env}.cloud.databricks.com',
                     http_path="sql/protocolv1/o/3808232419777503/0301-165641-8avwekvl",
                     access_token=token) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f'SELECT `Station Name` FROM {config.get("catalog")}.{config.get("schema")}.{config.get("table")} '
                f'GROUP BY `Station Name`, Line;'
            )
            result = cursor.fetchall()
    for row in result:
        print("LOGGING ALL KEYS")
        print(row[0])
        cluster_id = config.get("cluster_id")
        create_job(endpoint=f'https://ias-{env}.cloud.databricks.com/api/2.0/jobs/create', token=token, name=row[0], cluster_id=cluster_id)


if __name__ == '__main__':
    main()
