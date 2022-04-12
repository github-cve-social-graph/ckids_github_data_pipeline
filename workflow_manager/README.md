#Workflow Manager

This is simple workflow manager, where we can define the DAG of jobs

### Creating a simple DAG
1. Define configuration database properties in a json file
```commandline
{
        "mongo_url": "mongodb+srv://ckids:ckids@cluster0.4lols.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",
        "dag_collection": "dag_coll",
        "dag_name": "sample_dag"
}
```
2. Creating DAG
```commandline
import workflow_manager.workflow_tracker.DAG
 
d = DAG("dag_config.json", "example2")
d.next("job_1").next("job_2").parallel("job_3").next("job_4").create_dag()
d.persist_dag()
```

### Running a job

Each job communicates to the next job through payload, and data returned by job_main_function is passed on as payload to the next job.

To run a job in a defined DAG, below are the steps

1. Define config as below
```commandline
{
        "mongo_url": "mongodb+srv://ckids:ckids@cluster0.4lols.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",
        "workflow_collection": "workflow_coll",
        "dag_collection": "dag_coll",
        "version": "first_run",
        "dag_name": "example2",
        "job_name": "job_1"
}
```
2. Start the job by wrapping it with WorkflowTracker
```commandline
import workflow_manager.workflow_tracker.WorkflowTracker

workflow_manager = WorkflowTracker("./job_1_config.json", job_1_main_function)
workflow_manager.trigger_job()
```

