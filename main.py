import json
from databricks import sql


def main():
    """
    Testing dynamic workflows on Databricks
    :return:
    """
    config = json.loads(open("config.json").read())
    with sql.connect(server_hostname="https://ias-sandbox.cloud.databricks.com/",
                     http_path="sql/protocolv1/o/3808232419777503/0301-165641-8avwekvl",
                     access_token=dbutils.secrets.get(scope="workflow", key="databricks_token")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f'SELECT COUNT(DISTINCT station_name) FROM {config.get("catalog_name")}.{config.get("schema_name")}.{config.get("table_name")} '
                f'GROUP BY Line'
            )
            result = cursor.fetchall()
    for row in result:
        print(row)


if __name__ == '__main__':
    main()
