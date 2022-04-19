from model import Model
from user import User
from repository import Repository
from connection import getConnectionDriver
import json

driver = None

def createUserFromJson(userJson):
  user = User(driver,userJson["login"],userJson["company"])
  if not user.exists():
    user.save()
  return user

def linkAllRepository(orgName, repos):
  for i,repotoLInk in enumerate(repos):
    for j, curr in enumerate(repos):
      if i != j:
        repotoLInk.addRelationship(curr,orgName)

def createRepository(orgName, repoList):
  repos = []
  for repo in repoList:
    curr = Repository(driver,repo["name"], repo["createdAt"])
    if not curr.exists():
      curr.save()
    repos.append(curr)
  
  linkAllRepository(orgName,repos)
  return repos

def extractRepoFromOrgAndSave(orgJson):
  repo = []
  for repoEdges in orgJson["data"]["organization"]["repositories"]["edges"]:
    repoNode= {}
    repoNode["name"] = repoEdges["node"]["name"]
    repoNode["createdAt"] = repoEdges["node"]["createdAt"]
    repo.append(repoNode)
  
  return createRepository(orgJson["data"]["organization"]["name"], repo)

def linkUserToRepo(commitJson, relationship):
  user = User(driver,commitJson["data"]["user"]["login"], None)
  if not user.exists():
    user.save()
  repoName = commitJson["origin"].split("/")[-1]
  repo = Repository(driver,repoName,None)
  if not repo.exists():
    repo.save()
  user.addRelationship(repo, relationship)

def create_edges_vertexes():
  driver = getConnectionDriver("neo4j", "kaushik", "bolt://localhost:7687/")

  userData = open("./sample_data/user.json")
  orgData = open("./sample_data/organization.json")

  userJson = json.load(userData)
  orgJson = json.load(orgData)

  commitData = open("./sample_data/commit.json")
  commitJson = json.load(commitData)

  issueData = open("./sample_data/issue.json")
  issueJson = json.load(issueData)

  createUserFromJson(userJson)
  extractRepoFromOrgAndSave(orgJson)
  linkUserToRepo(commitJson, "COMMITS_TO")
  linkUserToRepo(issueJson, "RAISES_ISSUE")
  userData.close()
  orgData.close()
  commitData.close()
  issueData.close()

if __name__ == "__main__":
  wm_create_vertex_edges = WorkflowTracker("./components/neo4j_ingestor/wm_config/create_vertex_edge.json",
                             create_edges_vertexes)
  wm_create_vertex_edges.trigger_job()