import mongo_data_handler
import fetch_data as data
import json

def main():
    f = open('./config.json')
    config = json.load(f)
    f.close()
    organization = config["COMPANY"]
    from_year, from_month, from_day = config["FROMYEAR"], config["FROMMONTH"], config["FROMDAY"]
    to_year, to_month, to_day = config["TOYEAR"], config["TOMONTH"], config["TODAY"]
    response_org_id  = data.fetch_git_data().get_organization(organization)

    # reading from json payload
    payload_repos =  mongo_data_handler.MongoDataHandler().get_all_repos(organization) ## can be taken from config file for specific repos
    if response_org_id:
        for repo in payload_repos:

            # 1
            issues_reposne = data.fetch_git_data().get_perceval_data("Issue", organization, repo["name"], from_year, from_month, from_day, to_year, to_month, to_day )
            # 2
            commit_reposne = data.fetch_git_data().get_commit_data(repo["url"])

    # extract data from issues
   
    count = mongo_data_handler.MongoDataHandler().extract_user_data_from_issues()
    count1 = mongo_data_handler.MongoDataHandler().extract_user_data_from_commits()
    print('number of users extracted from commits'+ count1)
    print('number of users extracted from issues' + count)




if __name__ == "__main__":
    main()