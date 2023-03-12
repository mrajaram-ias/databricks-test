import json
import os
from databricks import sql



def main():
    """
    Testing dynamic workflows on Databricks
    :return:
    """
    config = json.loads(open("config.json").read())
    with sql.connect(server_hostname="dbc-f7060552-825c.cloud.databricks.com",
                     http_path="/sql/1.0/warehouses/7f5f47a2f194c89d",
                     access_token=os.getenv("DATABRICKS_TOKEN")) as connection:
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
