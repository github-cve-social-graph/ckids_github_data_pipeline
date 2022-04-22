from components.neo4j_ingestor.model import Model

class User(Model):

  def __init__(self, driver,name,company):
    super().__init__(driver)
    self.name = name
    self.company = company
    self.addPrimaryKey("name")