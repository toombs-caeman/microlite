# Microlite
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

a micro sqlite ORM

# Purpose
Let's answer the question 'How good can we make an ORM in <= 1000 lines?'


To start out, lets set down the base assumptions:
* use [black](https://pypi.org/project/black/) for formatting.
* use cloc to count lines (only code counts).
    * do not include tests in line count


# features
* a numpy like syntax for

* manage database options
    * reasonable defaults for testing in memory
* enhanced query logging
* automated migrations

# pre-release tasks
* add documentation / typing
* follow instructions https://docs.python.org/3/distutils/sourcedist.html
  * or https://setuptools.readthedocs.io/en/latest/setuptools.html
  * or poetry?
* badges?

# design
Every Model subclass represents a table (unless its name begins with _ in which case it is considered abstract).
That table's fields are determined by Field instances defined on the class.
A Model instance represents a query into that table.
Queries return a ModelRow subclass specific to that Model, also available as `Model.row`.
A ModelRow's fields are available as attributes, along with the methods `save` and `delete`.



`__repr__` is used to return strings needed to create the *sql* object, not the *python* object, as it is usually used.
`__str__` is used similarly to reference a sql object in a query. 
As such, `str(Model)` returns the table name, while `repr(Model)` returns a create table sql statement.
They are the same for a query object.

# TODO
* properly handle safe parameter injection `?` not `{}`
* `__sql__` not `__repr__`
