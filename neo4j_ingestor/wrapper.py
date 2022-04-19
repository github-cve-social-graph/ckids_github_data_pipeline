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

if __name__ == "__main__":
  driver = getConnectionDriver("neo4j","kaushik","bolt://localhost:7687/")

  userData = open("./sample_data/user.json")
  orgData = open("./sample_data/organization.json")
  
  userJson = json.load(userData)
  orgJson = json.load(orgData)

  user = createUserFromJson(userJson)
  repos = extractRepoFromOrgAndSave(orgJson)

  userData.close()
  orgData.close()
