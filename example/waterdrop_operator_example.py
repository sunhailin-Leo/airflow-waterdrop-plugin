from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_waterdrop_plugin.operators.waterdrop_operator import WaterDropOperator

with DAG(
    dag_id="waterdrop_test",
    start_date=days_ago(2),
) as dag:
    WaterDropOperator(
        # /data/waterdrop/waterdrop-1.5.1/bin/start-waterdrop.sh
        execute_path="<Your Waterdrop shell path>",
        config_save_path="<Your Config Folder>",
        config_save_name="hive-to-mongo.config",
        app_name=f"sync-hive-to-mongodb",
        executor_instances=6,
        executor_cores=6,
        executor_memory="12g",
        sql_catalog="hive",
        input_config={
            "hive": {
                "pre_sql": "SELECT * FROM test",
                "table_name": "temp_table"
            }
        },
        output_config={
            "mongodb": {
                "writeconfig.uri": "mongodb://root:123456@127.0.0.1:27001",
                "writeconfig.database": "test",
                "writeconfig.collection": "c_test_table"
            }
        }
    )
