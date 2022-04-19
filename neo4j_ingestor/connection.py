from neo4j import GraphDatabase

def getConnectionDriver(userName, password, connectionUrl):
  return GraphDatabase.driver(connectionUrl,auth=(userName,password))
