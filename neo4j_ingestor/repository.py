from model import Model

class Repository(Model):
  def __init__(self, driver,name,createdAt):
    super().__init__(driver)
    self.name = name
    self.createdAt = createdAt
    self.addPrimaryKey("name")