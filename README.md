# Microlite
a micro sqlite ORM

# Purpose
Let's answer the question 'How good can we make an ORM in <= 1000 lines?'


To start out, lets set down the base assumptions:
* use [black](https://pypi.org/project/black/) for formatting.
* The line count includes blank lines, but not tests.


# features
* manage database options
    * reasonable defaults for testing in memory
* enhanced query logging
* automated migrations

# feature tasks
* fix up default value handling. I'm inclined to handle it entirely python side
  since sqlite3's converter/adapter handling is atrocious.
* wrap/fix up converter/adapter/TYPES handling
* try to make it thread-safe by handling connections correctly


# pre-release tasks
* pre-commit hook
  * black + isort
  * coverage / badges?
  
* flesh out tests
* add documentation / typing
* follow instructions https://docs.python.org/3/distutils/sourcedist.html
  * or https://setuptools.readthedocs.io/en/latest/setuptools.html

# design
Every Model subclass represents a table (unless its name begins with '_' in which case it is considered abstract).
That table's fields are determined by Field instances defined on the class.
A Model instance represents a query into that table.
Queries return a ModelRow subclass specific to that Model, also available as `Model.row`.
A ModelRow's fields are available as attributes, along with the methods `save` and `delete`.



`__repr__` is used to return strings needed to create the *sql* object, not the *python* object, as it is usually used.
`__str__` is used similarly to reference a sql object in a query. 
As such, `str(Model)` returns the table name, while `repr(Model)` returns a create table sql statement.
They are the same for a query object.
