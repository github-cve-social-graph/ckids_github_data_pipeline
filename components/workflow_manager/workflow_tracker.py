import json
import time
from pymongo import MongoClient
from datetime import datetime
import threading


class WorkflowTracker:
    def __init__(self, config_path, job_func):
        self.config = self.load_config(config_path)
        self.initialized = False
        self.mongo_url = self.config["mongo_url"]
        self.client = MongoClient(self.mongo_url)
        self.db = self.client.CKIDS
        self.workflow_collection = self.db[self.config["workflow_collection"]]
        self.dag_collection = self.db[self.config["dag_collection"]]
        self.current_version = self.config["version"]
        self.dag_name = self.config["dag_name"]
        self.job_name = self.config["job_name"]
        self.job = job_func

    @staticmethod
    def load_config(config_path):
        with open(config_path, 'r') as openfile:
            return json.load(openfile)

    def set_next_flow_initialized_status(self, payload):
        myquery = {"dag": self.dag_name, "type": "linear", "current_job_name": self.job_name}
        result = list(self.dag_collection.find(myquery))
        if len(result) == 0:
            return
        if len(result) > 1:
            raise Exception("More than 1 job for same version")
        self.workflow_collection.insert_one(
            {"version": self.current_version, "job_name": result[0]["next_job_name"], "status": "INITIALISED",
             "payload": payload})

    def set_running_status(self):
        myquery = {"version": self.current_version, "job_name": self.job_name, "status": "INITIALISED"}
        newvalues = {"$set": {"status": "RUNNING"}}
        self.workflow_collection.update_one(myquery, newvalues)

    def set_completed_status(self):
        myquery = {"version": self.current_version, "job_name": self.job_name, "status": "RUNNING"}
        newvalues = {"$set": {"status": "COMPLETED"}}
        self.workflow_collection.update_one(myquery, newvalues)

    def get_current_payload(self):
        myquery = {"version": self.current_version, "job_name": self.job_name}
        result = list(self.workflow_collection.find(myquery))
        if len(result) > 1:
            raise Exception("More than 1 job for same version")
        return result[0]["payload"]

    def get_initialized_job(self):
        myquery = {"version": self.current_version, "job_name": self.job_name, "status": "INITIALISED"}
        result = list(self.workflow_collection.find(myquery))
        if len(result) == 0:
            init_query = {"dag": self.dag_name, "current_job_name": "flow_start", "next_job_name": self.job_name}
            init_result = list(self.dag_collection.find(init_query))
            if len(init_result) == 1 and self.initialized == False:
                self.workflow_collection.insert_one(
                    {"version": self.current_version, "job_name": self.job_name, "status": "INITIALISED",
                     "payload": ""})
                self.initialize_parallel_jobs()
                self.initialized = True
                return list({"version": self.current_version, "job_name": self.job_name, "status": "INITIALISED",
                             "payload": ""})
            else:
                return []
        if len(result) == 1:
            self.initialize_parallel_jobs()
            return list(result)
        elif len(result) > 0:
            raise Exception("More than 1 entry for same job for same version that is initialised")

    def initialize_parallel_jobs(self):
        parallel_job_query = {"dag": self.dag_name, "current_job_name": self.job_name, "type": "parallel"}
        parallel_job_result = list(self.dag_collection.find(parallel_job_query))
        print("parallel_job_query " + str(parallel_job_query) + " results: " + str(parallel_job_result))
        if len(parallel_job_result) == 1:
            self.workflow_collection.insert_one(
                {"version": self.current_version, "job_name": parallel_job_result[0]["parallel_job_name"],
                 "status": "INITIALISED", "payload": self.get_current_payload()})

    def trigger_job(self):
        while 1:
            time.sleep(5)
            initialized_job = self.get_initialized_job()
            if len(initialized_job) == 0:
                continue
            self.set_running_status()
            next_job_payload = self.job()
            self.set_completed_status()
            self.set_next_flow_initialized_status(next_job_payload)


def job_init_function():
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    time.sleep(5)
    print("Current Time is :", current_time)
    print("function running")


# def test_workflow_manager():
#     workflow_manager_1 = WorkflowTracker("./config_1.json", job_init_function)
#     workflow_manager_2 = WorkflowTracker("./config_2.json", job_init_function)
#     workflow_manager_3 = WorkflowTracker("./config_3.json", job_init_function)
#     workflow_manager_4 = WorkflowTracker("./config_4.json", job_init_function)
#
#     t1 = threading.Thread(target=workflow_manager_1.trigger_job)
#     t1.start()
#
#     t2 = threading.Thread(target=workflow_manager_2.trigger_job)
#     t2.start()
#
#     t3 = threading.Thread(target=workflow_manager_3.trigger_job)
#     t3.start()
#
#     workflow_manager_4.trigger_job()
#
#
# test_workflow_manager()
