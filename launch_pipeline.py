import os
from Components.workflow_manager.create_dag import create_ingestion_dag_if_not_exists
import subprocess

create_ingestion_dag_if_not_exists("./Components/API_data_ingestion/wm_config/issues_config.json")
subprocess.call(["python3", "./Components/API_data_ingestion/main.py"])
#os.system('python3 ./Components/API_data_ingestion/main.py')
