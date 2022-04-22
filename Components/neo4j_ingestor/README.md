# Neo4J Custom OGM (Object Graph Model)

## Motivation

Upon exploring neomodel and other OGM like py2neo, we were able to ascertain that currently no OGM supports all of the below features:

- Simple and readable syntax.
- Support for the latest neo4J 4.x instances.
- connection pooling support

As a solution to this, we thought of incorporating a simple OGM that would tackle all of the above features

## Syntax

Most of common actions are present in `Model.py`. An exhaustive list of methods are:
- exists()
- save()
- find()
- deleteNode()
- deleteAll(): static method to delete all nodes and edges in a database

To use the above model, you will need a driver connection instance. To understand more check `connection.py`.

- Create a class `Testmodel` that extends `Model` and then for each field in the class you will need to register the primaryKey and other fields with the same name (To be fixed/made more cleaner).eg:

```
class Testmodel(Model):
  def __init__(self, driver,field1,field2):
    super().__init__(driver)
    self.field1 = field1
    self.field2 = field2
    # register fields by adding the key fields of each node
    self.addPrimaryKey("field1")
    self.addPrimaryKey("field2")
```

- We need to register a primary key as the `find()` and `exists()` method finds/checks for existance by searching only the primary fields/keys.

- other fields can be added by calling addField.

- relationships can be added calling the `addRelationship` method. eg:

```
  model1 = Testmodel(driver,"field1", "field2")
  model2 = Testmodel(driver,"field3", "field4")
  model1.addRelationship(model2,"ANY_RELATIONSHIP_NAME")
```

## Bugs

- adding fields sometimes does not register the field and does not appear in the node information.

- registering fields is not very intuitive and if the strings are different from the actual variable names then an error is raised as the field does not exist.