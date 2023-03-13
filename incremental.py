import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_DATABASE = 'demo'
SNOWFLAKE_SCHEMA = 'prod_db'

SNOWFLAKE_ROLE = 'developer'
SNOWFLAKE_WAREHOUSE = 'demo1'
SNOWFLAKE_STAGE = 'demo_stage'

DAG_ID = "stock_history_increload"


with DAG(
    DAG_ID,
    start_date=datetime(2022, 10, 17),
    schedule_interval='0 8 * * 1-5',
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['group1'],
    catchup=False,
) as dag:

    snowflake_stock_history_fact = SnowflakeOperator(
        task_id="insurance_fact_incre",
        sql='''
            INSERT INTO fact_insurance_policy
                SELECT
                    policy_id,
                    ,policy_holder_id
                    ,receiver_id
                    ,start_date
                    ,end_date
                    ,status
                FROM PINGAN_SICHUAN.PUBLIC.POLICY_PRE_STAGE
                WHERE date='{{ ds }}';
            ''',
    )

    snowflake_company_profile_dim = SnowflakeOperator(
        task_id='company_profile_incre',
        sql='./company_profile_incre.sql',
    )


    (
        [
            snowflake_company_profile_dim >> snowflake_stock_history_fact

        ]

    )