import components.API_data_ingestion.mongo_data_handler as mongo_data_handler
import components.API_data_ingestion.fetch_data as data
import json
import threading
from components.workflow_manager.workflow_tracker import WorkflowTracker


def ingest_issues():
    f = open('./components/API_data_ingestion/config.json')
    config = json.load(f)
    f.close()
    organization = config["COMPANY"]
    from_year, from_month, from_day = config["FROMYEAR"], config["FROMMONTH"], config["FROMDAY"]
    to_year, to_month, to_day = config["TOYEAR"], config["TOMONTH"], config["TODAY"]
    response_org_id = data.fetch_git_data().get_organization(organization)
    # reading from json payload
    payload_repos = mongo_data_handler.MongoDataHandler().get_all_repos(
        organization)  ## can be taken from config file for specific repos
    if response_org_id:
        for repo in payload_repos:
            issues_reposne = data.fetch_git_data().get_perceval_data("issue", organization, repo["name"], from_year,
                                                                     from_month, from_day, to_year, to_month, to_day)

    # extract data from issues


def ingest_commits():
    f = open('./components/API_data_ingestion/config.json')
    config = json.load(f)
    f.close()
    organization = config["COMPANY"]
    from_year, from_month, from_day = config["FROMYEAR"], config["FROMMONTH"], config["FROMDAY"]
    to_year, to_month, to_day = config["TOYEAR"], config["TOMONTH"], config["TODAY"]

    response_org_id = data.fetch_git_data().get_organization(organization)

    # reading from json payload
    payload_repos = mongo_data_handler.MongoDataHandler().get_all_repos(
        organization)  ## can be taken from config file for specific repos
    if response_org_id:
        for repo in payload_repos:
            commit_reposne = data.fetch_git_data().get_commit_data(repo["url"], repo["name"])


def get_users_from_issues_commits():
    count = mongo_data_handler.MongoDataHandler().extract_user_data_from_issues()
    count1 = mongo_data_handler.MongoDataHandler().extract_user_data_from_commits()
    print('number of users extracted from commits' + str(count1))
    print('number of users extracted from issues' + str(count))


wm_issues = WorkflowTracker("./components/API_data_ingestion/wm_config/issues_config.json", ingest_issues)
wm_commits = WorkflowTracker("./components/API_data_ingestion/wm_config/commits_config.json", ingest_commits)
wm_users = WorkflowTracker("./components/API_data_ingestion/wm_config/user_extraction_config.json",
                           get_users_from_issues_commits)

#
# print("Starting Issues Job Deamon\n")
# t1 = threading.Thread(target=wm_issues.trigger_job)
# t1.start()
#
# print("Starting  Commit Job Deamon\n")
# t2 = threading.Thread(target=wm_commits.trigger_job)
# t2.start()

print("Starting User Extraction Job Deamon\n")
wm_users.trigger_job()
