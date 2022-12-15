#

# Licensed to the Apache Software Foundation (ASF) under one

# or more contributor license agreements.  See the NOTICE file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache..org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# under the License.


"""Example DAG demonstrating the usage of the BashOperator."""

import time
import csv
from pprint import pprint
from datetime import timedelta
from airflow import DAG
#from airflow.operators.bash import BashOperator

#from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

from airflow.utils.dates import days_ago

#from airflow.providers.sftp.operators import sftp_operator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor

from airflow.providers.mysql.operators.mysql import MySqlOperator


args = {
    'owner': 'airflow',
    'mysql_conn_id': 'maria_nas'
}

with DAG(
    dag_id='sftp_test',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2)
    #dagrun_timeout=timedelta(minutes=60),
    #tags=['example', 'example3'],
    #params={"example_key": "example_value"},
) as dag:

   

     # [START howto_operator_python]

    def print_context(ds, **kwargs):

        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)

    run_this = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )

    # [END howto_operator_python]

 

    def process_file(**kwargs):
        templates_dict = kwargs.get("templates_dict")
        input_file = templates_dict.get("input_file")       
        output_file = templates_dict.get("output_file")
        output_rows =[]
        with open(input_file,'r') as csv_file:
            spreader = csv.reader(csv_file,delimiter=',') 
            for row in spreader:
                row.append("processed")
        #        print(','.join(row))
                output_rows.append(row)
        with open(output_file,"w",newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerows(output_rows)

    wait_for_input_file = SFTPSensor(task_id="check-for-file",
                                     sftp_conn_id = "sftp_test2",
                                     path = "/friend/input.csv")
                                     ###poke_internal=10)
   
    download_file = SFTPOperator(
        task_id="get-file",
        ssh_conn_id = "sftp_test2",
        remote_filepath = "/friend/input.csv",
        local_filepath = "input.csv",
        operation ="get",
        create_intermediate_dirs = True
    )

    process_file = PythonOperator(task_id="process-file",
                                  templates_dict={
                                      "input_file":"input.csv",
                                      "output_file":"output.csv"
                                  },
                                  python_callable = process_file)
 

    # [START howto_operator_mysql]
    ###sql = ["insert into t_cust values ('1','kt')"]
    sql ="LOAD DATA LOCAL INFILE '/opt/airflow/input.csv' INTO TABLE t_cust FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;"
    insert_table_mysql_task = MySqlOperator(task_id='insert_table_mysql', sql=sql,autocommit=True,dag=dag)
###    insert_table_mysql_task.run(custid='1',custname='2')

    ###insert_table_mysql_task = MySqlOperator(task_id='insert_table_mysql',
    ###                                      sql=r"""insert into t_cust values('X') ;""", dag=dag)
    ###run_this_last = DummyOperator(
    ###    task_id='run_this_last',
    ###)

    # [START howto_operator_bash]
    ###run_this = BashOperator(
    ###    task_id='run_after_loop',
    ###    bash_command='echo 1',
    ###)

    # [END howto_operator_bash]
 

    run_this >> wait_for_input_file >> download_file >> process_file >> insert_table_mysql_task
 

    ###for i in range(3):
    ###    task = BashOperator(
    ###        task_id='runme_' + str(i),
    ###        bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
    ###    )
    ###    task >> run_this


    # [START howto_operator_bash_template]
    ###also_run_this = BashOperator(
    ###    task_id='also_run_this',
    ###    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    ###)

    # [END howto_operator_bash_template]
    ###also_run_this >> run_this_last

# [START howto_operator_bash_skip]
###this_will_skip = BashOperator(
   ### task_id='this_will_skip',
    ###bash_command='echo "hello world"; exit 99;',
    ###dag=dag,
###)

# [END howto_operator_bash_skip]

if __name__ == "__main__":

    dag.cli()
