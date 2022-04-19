import os
from components.workflow_manager.create_dag import create_ingestion_dag_if_not_exists
import subprocess

create_ingestion_dag_if_not_exists("./components/API_data_ingestion/wm_config/issues_config.json")
subprocess.call(["python3.9", "-m", "components.API_data_ingestion.main"])
subprocess.call(["python3.9", "-m", "components.neo4j_ingestor.wrapper"])
#os.system('python3 ./components/API_data_ingestion/main.py')
