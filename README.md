# Microlite
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)

a micro sqlite object-relational mapping (ORM) for python.

# Purpose
Let's answer the question 'How good can we make an ORM in <= 1000 lines?'

To start out, lets set down the base assumptions:
* use [black](https://pypi.org/project/black/) for formatting.
* use cloc to count lines (only code counts).
    * do not include tests in line count

# Features
* initialize sqlite tables from python class definitions
* automated migrations when python definitions change (protected by a flag).
* wraps returned rows as python objects
* automatically fetch foreign key relationships on attribute access

# Examples
Define sqlite tables by defining python classes. Then initialize the database `file.db`.
```python
from microlite import Model, Field, initialize_database
import datetime

# Model subclasses that start with '_' will not be translated into sqlite tables
class _Person(Model):
    name = Field(str, "NA", notnull=True)
    birthday = Field(datetime.date, datetime.date(1000, 1, 1), notnull=True)

class Collector(_Person):
    net_worth = Field(float)

class Artist(_Person):
    style = Field(str, "Unknown")

class Painting(Model):
    name = Field(str)
    artist = Field(Artist)
    list_price = Field(float)

    def __str__(self):
        return f"{self.name!r} by {self.artist} worth {self.list_price}"

class Sale(Model):
    date = Field(datetime.date)
    painting = Field(Painting)
    artist = Field(Artist)
    collector = Field(Collector)
    price = Field(float)

# initialize_database raises an exception if the defined classes don't match the
# table definitions and migrations aren't allowed
initialize_database('file.db', allow_migrations=True)
```

Create a new object and save it in the database.
```python
# create a new object and then save it to the database()
a = Artist(name="Johannes Vermeer").save()
a.style == "Unknown"  # True
```

Iterate over all artists of the baroque style
```python
for artist in Artist.style == "Baroque":
    ...

# get the same rows as a list
baroque_artists = list(Artist.style == "Baroque")
```

# Design
Every Model subclass represents a table (unless its name begins with _ in which case it is considered abstract).
That table's fields are determined by Field instances defined on the class.
A Model instance represents a row of that table. Model.save() will save the row to the database.

Accessing attributes of the class (not the instance) will usually generate a Query which can accumulate clauses through a pythonic syntax. Iterating over a Query will execute the query and return a series of rows as the applicable Model instance.

# TODO
* properly handle safe parameter injection (this mostly works)
* handle all valid SQL clauses
* thread safety?
* select with foreign key. Execute a single query to pre-fetch foreign key attributes of the returned Model.
* expose better debug info from sqlite
