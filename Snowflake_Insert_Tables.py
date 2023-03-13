import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator



SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'test_database'
SNOWFLAKE_SCHEMA = 'test_schema'

SNOWFLAKE_ROLE = 'AW_developer'
SNOWFLAKE_WAREHOUSE = 'aw_etl'
SNOWFLAKE_STAGE = 'test_stage'


SNOWFLAKE_SAMPLE_TABLE = 'airflow_testing'

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
#more dymanic no hard coding
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
#put sql queries into a list 10 times and convert it into a string


# ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "Snowflake_Insert_Table"

with DAG(
        DAG_ID,
        start_date=datetime(2021, 1, 1),
        schedule_interval='30 * * * *',
        #update  every 30 minutes
        #https://crontab.guru/
        default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
        tags=['test'],
        catchup=False,
        #True = startdate excute dags that were not excuted before, but cant exceed the start_date
) as dag:
    # [START snowflake_example_dag]
    snowflake_op_sql_str = SnowflakeOperator(
        task_id='snowflake_op_sql_str',
        sql=CREATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

    snowflake_op_with_params = SnowflakeOperator(
        task_id='snowflake_op_with_params',
        sql=SQL_INSERT_STATEMENT,
        parameters={"id": 5},
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_op_sql_list = SnowflakeOperator(task_id='snowflake_op_sql_list', sql=SQL_LIST)

    snowflake_op_sql_multiple_stmts = SnowflakeOperator(
        task_id='snowflake_op_sql_multiple_stmts',
        sql=SQL_MULTIPLE_STMTS,
    )

    snowflake_op_template_file = SnowflakeOperator(
        task_id='snowflake_op_template_file',
        sql='./test.sql',
    )


    (
            snowflake_op_sql_str
            >> [
                snowflake_op_with_params,
                snowflake_op_sql_list,
                snowflake_op_template_file,
                # copy_into_table,
                snowflake_op_sql_multiple_stmts,
            ]

    ) #dependncy， excute tasks parrallely in the []
    # [END snowflake_example_dag]
