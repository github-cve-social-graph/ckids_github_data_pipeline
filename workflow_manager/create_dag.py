import json
from pymongo import MongoClient


class DAG:
    def __init__(self, config_path, dag_name):
        self.dag_name = dag_name
        self.dag = []
        self.current = []
        self.config = self.load_config(config_path)
        self.mongo_url = self.config["mongo_url"]
        self.client = MongoClient(self.mongo_url)
        self.db = self.client.CKIDS
        self.dag_collection = self.db[self.config["dag_collection"]]

    @staticmethod
    def load_config(config_path):
        with open(config_path, 'r') as openfile:
            return json.load(openfile)

    def next(self, job_name):
        if len(self.current) > 0:
            self.dag.append(self.current)
            self.current = []
        self.current.append(job_name)
        return self

    def parallel(self, job_name):
        self.current.append(job_name)
        return self

    def create_dag(self):
        if len(self.current) > 0:
            self.dag.append(self.current)

    def insert_parallel_job_entry(self, job_name, parallel_job_name):
        self.dag_collection.insert_one({"dag": self.dag_name, "type": "parallel", "current_job_name": job_name,
                                   "parallel_job_name": parallel_job_name})

    def insert_next_job_entry(self, job_name, next_job_name):
        self.dag_collection.insert_one(
            {"dag": self.dag_name, "type": "linear", "current_job_name": job_name, "next_job_name": next_job_name})

    def persist_dag(self):
        last_ele = "flow_start"
        for i in range(len(self.dag)):
            if len(self.dag[i]) > 1:
                self.insert_next_job_entry(last_ele, self.dag[i][0])
                for j in range(len(self.dag[i]) - 1):
                    self.insert_parallel_job_entry(self.dag[i][j], self.dag[i][j+1])
                    last_ele = self.dag[i][j+1]
            else:
                self.insert_next_job_entry(last_ele, self.dag[i][0])
                last_ele = self.dag[i][0]


def test_dag_creation():
    d = DAG("dag_config.json", "example2")
    d.next("job_1").next("job_2").parallel("job_3").next("job_4").create_dag()
    d.persist_dag()
    print(d.dag)


test_dag_creation()
