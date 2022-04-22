from components.neo4j_ingestor.model import Model
from components.neo4j_ingestor.user import User
from components.neo4j_ingestor.repository import Repository
from components.neo4j_ingestor.connection import getConnectionDriver
import json
import pymongo
from components.workflow_manager.workflow_tracker import WorkflowTracker



def createUserFromJson(userJson):
  user = User(driver, userJson["login"], userJson["company"])
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
    curr.setOrganization(orgName)
    if not curr.exists():
      curr.save()
    repos.append(curr)
  
  # linkAllRepository(orgName,repos)
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
  global driver
  driver = getConnectionDriver("neo4j", "WXffnV2OwnKrodjVEXa_DNQfaevuja01nUTVjzjAaNQ", "neo4j+s://04cf9446.databases.neo4j.io:7687")

  config = json.load(open("./components/neo4j_ingestor/wm_config/create_vertex_edge.json"))
  mongo_url = config["mongo_url"]
  client = pymongo.MongoClient(mongo_url)
  db = client["CKIDS"]

  for i in list(db["user"].find({})):
    print(i)
    print(type(i))
    if i == None:
      continue
    userJson = json.dumps(i, default=str)
    createUserFromJson(i)
  #
  # for i in list(db["Organization"].find({})):
  #   if i == None:
  #     continue
  #   orgJson = json.dumps(i, default=str)
  #   extractRepoFromOrgAndSave(i)


  print("issue")
  for i in list(db["issue"].find({})):
    print(i)
    print("\n")
    if i == None:
      continue
    issueJson = json.dumps(i, default=str)
    linkUserToRepo(i, "RAISES_ISSUE")
  #
  # print("commit")
  # for i in list(db["commit"].find({})):
  #   print(i)
  #   print("\n")
  #   if i == None:
  #     continue
  #   commitJson = json.dumps(i, default=str)
  #   linkUserToRepo(i, "COMMITS_TO")

  # userData = open("./sample_data/user.json")
  # orgData = open("./sample_data/organization.json")
  #
  # userJson = json.load(userData)
  # orgJson = json.load(orgData)

  # commitData = open("./sample_data/commit.json")
  # commitJson = json.load(commitData)

  # issueData = open("./sample_data/issue.json")
  # issueJson = json.load(issueData)

  # userData.close()
  # orgData.close()
  # commitData.close()
  # issueData.close()

if __name__ == "__main__":
  print("Starting neo4j ingestor deamon")
  wm_create_vertex_edges = WorkflowTracker("./components/neo4j_ingestor/wm_config/create_vertex_edge.json",
                             create_edges_vertexes)
  wm_create_vertex_edges.trigger_job()