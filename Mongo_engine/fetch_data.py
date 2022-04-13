from numpy import number
from perceval.backends.core.git import Git
from perceval.backends.core.github import GitHub as PercevalGithub
import json
from datetime import datetime, timezone
import mongo_data_handler as repo
import requests


class fetch_git_data:
    def __init__(self) -> None:
        f = open('./config.json')
        data = json.load(f)
        f.close()
        self.token = data["GITHUB_ACCESS_TOKEN"]
        self.base_url = data["REST_BASE_API"]
        self.header = {'Authorization': "Bearer "+ self.token, "Accept": "application/vnd.github.v3+json"}
        self.graphql_url = data["GRAPHQL_API"]

    #issues
    def get_perceval_data(self, category, owner, _repo, from_year, from_month, from_day, to_year, to_month, to_day):
        from_date = datetime(from_year, from_month, from_day, tzinfo=timezone.utc)
        to_date = datetime(to_year, to_month, to_day, tzinfo=timezone.utc)
        prg = PercevalGithub(owner=owner, repository=_repo, api_token=[self.token])
        data =  list(prg.fetch(category=category, from_date=from_date, to_date=to_date))
        for d in data:
            res = repo.MongoDataHandler().save_response(d, category)
            if res:
                print("saved")
            else:
                print("not saved")

    def get_commit_data(self, repo_url, repo_name):            
        repo_url = repo_url
        repo_dir = '/tmp/'+repo_name+'.git'
        reposiotry = Git (uri=repo_url, gitpath=repo_dir)
        count =0
        for commit in reposiotry.fetch():
            count+=1
            re = repo.MongoDataHandler().save_response(commit, "commit")
        return count

    def get_user(self,user):
        api_url = self.base_url+"users/"+user
        
        response = requests.get(api_url, headers= self.header)
        r = response.json()
        return r

    def get_organization(self,org):
        _url = self.graphql_url
        response_arr = []
        _query = '''query { organization(login:"'''+ org +'''") { id location name email databaseId description repositories(first: 1) { pageInfo { endCursor hasNextPage } edges { node { id name createdAt languages(first:3) {edges { node { name } } } url labels(first:5){ edges{ node{ name } } } } } } } }'''
        response = requests.post(_url, json={'query': _query}, headers= self.header)
        r = response.json()
        
        response_arr.append(r)
        hasNextPage= r["data"]["organization"]["repositories"]["pageInfo"]["hasNextPage"]
        while hasNextPage:
            endCur = r["data"]["organization"]["repositories"]["pageInfo"]["endCursor"]
            _query = '''query { organization(login:"'''+ org +'''") { id location name email databaseId description repositories(first: 100, after: "''' +endCur+'''") { pageInfo { endCursor hasNextPage } edges { node { id name createdAt languages(first:3) {edges { node { name } } } url labels(first:5){ edges{ node{ name } } } } } } } }'''
            _response = requests.post(_url, json={'query': _query}, headers= self.header)
            r = _response.json()
            hasNextPage = r["data"]["organization"]["repositories"]["pageInfo"]["hasNextPage"]
            
            response_arr.append(r)
        final_response = self.construct_response(response_arr)
        id = repo.MongoDataHandler().save_response(final_response, "Organization")
        return id
    
    def get_repos(self, org):
        api_url = self.base_url+"/org/"+org+"/repos"
        response = requests.get(api_url, headers= self.header)
        r = response.json()
        for rp in r:
            repo.MongoDataHandler().save_response(rp,"repos")
        return r

    def construct_response(self, response_array):
        final_response = response_array[0]
        for i in range(1,len(response_array)):
            final_response["data"]["organization"]["repositories"]["edges"].extend(response_array[i]["data"]["organization"]["repositories"]["edges"])
        return final_response
    

    def grapql_get_commit_comments_user(self, org, _repo, first_number):
        _url = _url = self.graphql_url
        _query = '''query { repository(owner:"'''+ org+'''", name:"'''+ _repo+'''"){  owner { id login }collaborators { edges { node { id company email bio twitterUsername location } } } commitComments(first:'''+first_number+''') { edges { node { id commit { id author { date email name user { id login location twitterUsername bio company bioHTML createdAt } } } } } }'''
        response = requests.post(_url, json={'query': _query}, headers= self.header)
        r = response.json()
        for rp in r:
            repo.MongoDataHandler().save_response(rp,"commit_comments")
        return r