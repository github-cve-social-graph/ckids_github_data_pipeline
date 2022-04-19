from abc import abstractmethod
import json
import re

class Model:
  def __init__(self, driver):
    self.primary = []
    self.fields = []
    self.relationships = []
    self.driver = driver
    self.node = None
  
  def addPrimaryKey(self, fieldName):
    self.primary.append(fieldName)
    self.fields.append(fieldName)

  def addField(self, fieldName):
    self.fields.append(fieldName)
  
  def prepareFilterJson(self):
    primaryMap = {}
    for primaryKey in self.primary:
      primaryMap[primaryKey] = "$"+self.getType()+primaryKey
    return json.dumps(primaryMap)

  def prepareFilterJsonWithSuffix(self,suffix):
    primaryMap = {}
    for primaryKey in self.primary:
      primaryMap[primaryKey] = "$"+self.getType()+primaryKey+suffix
    return json.dumps(primaryMap)

  def getType(self):
    return self.__class__.__name__
  
  def addRelationship(self, other, relationship):
    _varA = "_"+self.getType()+"A"
    _typeA = self.getType()
    paramMapA = self.getParamMapWithSuffix("1")
    _filterA = re.sub(r'"(.*?)"', r'\1', self.prepareFilterJsonWithSuffix("1"))

    _varB = "_"+other.getType()+"B"
    _typeB = other.getType()
    paramMapB = other.getParamMapWithSuffix("2")
    _filterB = re.sub(r'"(.*?)"', r'\1', other.prepareFilterJsonWithSuffix("2"))

    paramMap = paramMapA | paramMapB
    with self.driver.session() as session:
      session.run(f'MATCH ({_varA}:{_typeA} {_filterA}) MATCH ({_varB}:{_typeB} {_filterB}) MERGE ({_varA})-[:{relationship}]->({_varB})',paramMap).consume()

  def deleteNode(self):
    _var = "_"+self.getType()
    _type = self.getType()
    paramMap = self.getParamMap()
    _filter = re.sub(r'"(.*?)"', r'\1', self.prepareFilterJson())
    with self.driver.session() as session:
      session.run(f'MATCH ({_var}:{_type} {_filter}) DETACH DELETE {_var}',paramMap).consume()

  def find(self):
    _var = "_"+self.getType()
    _type = self.getType()
    paramMap = self.getParamMap()
    _filter = re.sub(r'"(.*?)"', r'\1', self.prepareFilterJson())
    with self.driver.session() as session:
      self.node = session.run(f'MATCH ({_var}:{_type} {_filter}) RETURN {_var}',paramMap).single()
    return self.node

  def getFilter(self):
    primaryMap = {}
    for primaryKey in self.primary:
      primaryMap[primaryKey] = "$"+self.getType()+primaryKey
    return primaryMap
  
  def getFilterWithSuffix(self,suffix):
    primaryMap = {}
    for primaryKey in self.primary:
      primaryMap[primaryKey] = "$"+self.getType()+primaryKey+suffix
    return primaryMap

  def getParamMap(self):
    paramMap = {}
    for key,value in self.getFilter().items():
      paramMap[value[1:]] = vars(self)[key]
    return paramMap
  
  def getParamMapWithSuffix(self, suffix):
    paramMap = {}
    for key,value in self.getFilterWithSuffix(suffix).items():
      paramMap[value[1:]] = vars(self)[key]
    return paramMap

  def exists(self):
    _var = "_"+self.getType()
    _type = self.getType()
    paramMap = self.getParamMap()
    _filter = re.sub(r'"(.*?)"', r'\1', self.prepareFilterJson())
    
    with self.driver.session() as session:
      returnList = session.run(f'MATCH ({_var}: {_type} {_filter}) RETURN {_var}',paramMap).values()
      return len(returnList) > 0

  def save(self):
    _var = "_"+self.getType()
    _type = self.getType()
    paramMap = self.getParamMap()
    _filter = re.sub(r'"(.*?)"', r'\1', self.prepareFilterJson())
    with self.driver.session() as session:
      session.run(f'CREATE ({_var}:{_type} {_filter}) RETURN {_var}', paramMap).consume()

  @staticmethod
  def deleteAll(tx):
    tx.run("MATCH (n) DETACH DELETE n").consume()
  
