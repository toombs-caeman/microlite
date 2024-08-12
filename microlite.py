import logging
import datetime
import sqlite3
from enum import StrEnum
from abc import ABCMeta, abstractmethod
import typing
import copy
from itertools import chain

log = logging.getLogger(__name__)

"""

aggregation
* count, avg, count(*), sum, min, max, total

imagine tables as sets
* inner join - intersection (&)
* outer join - union (|)
* cross join - multiply (*)
* left  join - (+)
"""


def registerType(type, to_sql, from_sql):
    # wrap type registration to be a little easier to read
    sqlite3.register_adapter(type, to_sql)
    sqlite3.register_converter(type.__name__, from_sql)


# generate sql and params for connection.execute()
def SQL(o) -> str:
    if hasattr(o, "__sql__"):
        return o.__sql__()
    if isinstance(o, (int, float)):
        return str(o)
    return "(?)"


def params(o) -> tuple:
    """generate flattened tuple of exactly the arguments replaced with '(?)' by SQL()."""
    if hasattr(o, "__params__"):
        return o.__params__()
    if SQL(o) == "(?)":
        return (o,)
    return ()


class DoesNotExist(Exception):
    """At least one row was expected but does not exist."""


class TooManyExist(Exception):
    """Query matched multiple rows in a context where only zero or one was expected."""


class MigrationError(Exception):
    """The database could not be initialized because it does not match the python table definitions."""


import functools


class method:
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, type=None):
        _self = type if obj is None else obj
        return functools.partial(self.f, _self)


class QueryAPI(metaclass=ABCMeta):
    """Define the API used to construct SQL queries."""

    @abstractmethod
    def __pos__(self) -> "Query":
        """convert any conforming object to query."""

    @method
    def __len__(self) -> int:
        """the number of rows that match."""
        return len(+self)

    @method
    def __iter__(self) -> sqlite3.Cursor | typing.Iterator:
        """fetch rows."""
        return iter(+self)

    @method
    def __getitem__(self, __o) -> "Query":
        """restrict the query with additional WHERE clauses."""
        return (+self)[__o]

    @method
    def __call__(self, *columns, distinct=False) -> "Query":
        """specify additional columns."""
        return (+self)(*columns, distinct=distinct)

    @method
    def __contains__(self, __o) -> "Query":
        """fetch rows."""
        return (+self).__contains__(__o)

    @method
    def sort(self, *by):
        """specify an additional ORDER BY clause."""
        return (+self).sort(*by)

    @method
    def delete(self):
        """delete matching rows and return the number of rows deleted"""
        return (+self).delete()

    @method
    def first(self):
        """get the first matching row, or raise an error."""
        return (+self).first()

    @method
    def all(self):
        """get all matching rows"""
        return (+self).all()

    @method
    def get(self):
        """get the only matching row, or raise an error."""
        return (+self).get()

    # @abstractmethod
    # def __sql__(self) -> str:
    #    """return a sql fragment with proper parameter markers."""

    # @abstractmethod
    # def __params__(self) -> T.Iterable:
    #    """return a flat iterable with parameters matching the parameter markers from __sql__."""


class Query[T](QueryAPI):
    """
    top level query operations
    * select - return iter of Model()
    * select tuple - return only selected columns as iter of Row()
    * select deep - follow foreign keys and instantiate models
    * get_or_create - like django's get_or_create, return Model()
    * get - return Model() or raise count exception
    * delete - delete selected rows
    * explain

    collect all options in .__flags as python/extensible forms
    let property(__clause) be the sql-formatted clause for the corresponding flags
    let __curry() determine how flags are appended/extended
    """

    def __init__(
        self,
        table: T,
        columns=(),
        where=(),
        order=(),
        distinct=False,
        limit=None,
        offset=None,
    ):
        f = dict(
            table=table,
            columns=columns,
            where=where,
            order=order,
            distinct=distinct,
            limit=limit,
            offset=offset,
        )
        tup = "columns", "where", "order"
        for t in tup:
            if not isinstance(f[t], (list, tuple)):
                f[t] = (f[t],)
        self.__flags = f

    def __curry(self, extend=("where", "columns", "order"), **flags) -> "Query":
        """Used internally to add clauses and return a new Query object."""
        out = self.__flags.copy()
        for k, v in flags.items():
            if k in extend:
                out[k] += v
            else:
                out[k] = v
        return type(self)(**out)

    ## FRAGMENTS
    # properties used internally to build queries

    def __table(self):
        return SQL(self.__flags["table"])

    def __distinct(self):
        return " DISTINCT" if self.__flags["distinct"] else ""

    def __columns(self):
        c = self.__flags["columns"]
        return ", ".join(map(SQL, c)) if c else "*"

    def __where(self):
        w = self.__flags["where"]
        return f" WHERE {' AND '.join(SQL(wc) for wc in w)}" if w else ""

    def __order(self):
        o = self.__flags["order"]
        return f" ORDER BY {', '.join(SQL(oc) for oc in o)}" if o else ""

    def __group(self):
        return (
            c.by
            for c in self.__flags["columns"]
            if isinstance(c, Agg) and c.by is not None
        )

    def __sgroup(self):
        groups = tuple(self.__group())
        return f" GROUP BY {','.join(map(SQL, groups))}" if groups else ""

    def __limit(self):
        l = self.__flags["limit"]
        o = self.__flags["offset"]
        if l is None and o is None:
            return ""
        if o is None:
            return f" LIMIT {SQL(l)}"
        if l is None:
            return f" LIMIT {SQL(o)}, -1"
        return f" LIMIT {SQL(o)}, {SQL(l)}"

    def __select(self):
        return (
            f"SELECT{self.__distinct()} {self.__columns()} FROM {self.__table()}"
            f"{self.__where()}{self.__sgroup()}{self.__order()}{self.__limit()}"
        )

    def select(self):
        with self.__connection as c:
            log.info(repr(self.__flags))
            ret = c.execute(self.__select(), tuple(params(self)))
        if len(self.__flags["columns"]) == 1:
            return (x[0] for x in ret)
        return ret

    ## PROPERTIES
    def __sql__(self):
        return f"({self.__select()})"

    def __params__(self):
        # the order of parameters should match the order in __select
        return chain.from_iterable(
            map(
                params,
                chain(self.__flags["columns"], self.__group(), self.__flags["where"]),
            )
        )

    @property
    def __connection(self):
        if len(self.__flags["columns"]):
            return Model._connection
        return self.__flags["table"]._connection

    ## API
    def __pos__(self):
        return self

    def __iter__(self) -> typing.Iterator[T]:
        return iter(self.select())

    def __len__(self) -> int:
        return self.__curry(columns=Agg("COUNT"), extend="where").get()

    def __repr__(self):
        return f"Query{self.__sql__()}"

    def __getitem__(self, __o: object) -> "Query[T]" | T:
        """Create a new Query with an additional WHERE constraint or a LIMIT clause."""
        match __o:
            case slice():
                return self.__curry(offset=__o.start, limit=__o.stop)
            case int():
                return self.__curry(offset=__o, limit=1).get()
            case tuple():
                return self.__curry(where=__o)
            case _:
                return self.__curry(where=(__o,))

    def __call__(self, *columns, distinct=False):
        """
        Create a new Query with additional columns.

        Specifying the columns in this way changes the return type of the query from a Model instance to a tuple
        """
        return self.__curry(
            columns=columns, distinct=self.__flags["distinct"] or distinct
        )

    def __contains__(self, __o):
        return Query(self.__table, where=Filter(__o, cmp.in_, self))

    def delete(self) -> int:
        sql = f"DELETE FROM {self.__table()} {self.__where()}"
        with self.__connection as c:
            return c.execute(sql, tuple(params(self))).rowcount

    def sort(self, *by):
        return self.__curry(order=by)

    def first(self) -> T:
        first = next(iter(self[:1]), None)
        if first is None:
            raise DoesNotExist
        return first

    def all(self) -> list[T]:
        return list(self)

    def get(self) -> T:
        """Get a single row or raise an error."""

        i = iter(self[:2])
        first = next(i, None)
        if first is None:
            raise DoesNotExist
        second = next(i, None)
        if second is not None:
            raise TooManyExist
        return first


class Filter:
    def __init__(self, left, op, right=None):
        if right is None:
            op, right = left, op
            left = None
        self.left = left
        self.op = op
        self.right = right

    def __sql__(self):
        if self.left is None:
            return f"{SQL(self.op)}{SQL(self.right)}"
        return f"{SQL(self.left)} {SQL(self.op)} {SQL(self.right)}"

    def __repr__(self):
        return repr(SQL(self))

    def __params__(self):
        return (*params(self.left), *params(self.op), *params(self.right))


class cmp(StrEnum):
    eq = "="
    lt = "<"
    gt = ">"
    le = "<="
    ne = "<>"
    ge = ">="
    in_ = "IN"

    def __sql__(self):
        return str(self)

    def __params__(self):
        return ()


class do(StrEnum):
    """Flags that control what happens when a foreign key is updated or deleted."""

    nothing = "no action"
    restrict = "restrict"
    null = "set null"
    default = "set default"
    cascade = "cascade"


class conflict(StrEnum):
    rollback = "ROLLBACK"
    abort = "ABORT"
    fail = "FAIL"
    ignore = "IGNORE"
    replace = "REPLACE"


class Reference:
    """this is needed for type checking."""


class Field[T, F](QueryAPI):
    def __init__(
        self,
        typ: type[F],
        default: typing.Any = None,
        primary: bool | conflict = False,
        notnull: bool | conflict = False,
        delete: do = do.nothing,
        update: do = do.nothing,
        generate: str | None = None,
        store: bool = False,
    ):

        typename = (
            f"INTEGER REFERENCES {SQL(typ)}"
            if issubclass(typ, Reference)
            else {
                type(None): "NULL",
                int: "INTEGER",
                float: "REAL",
                str: "TEXT",
                bytes: "BLOB",
            }.get(typ, typ.__name__)
        )
        self.__name = None
        self.__type = typ

        self.__default = default
        default_prep = sqlite3.adapters.get(
            (type(default), sqlite3.PrepareProtocol), lambda x: x
        )(default)

        self.__create = " ".join(
            value
            for condition, value in [
                (True, typename),
                self.__conflict(delete, "ON DELETE"),
                self.__conflict(update, "ON UPDATE"),
                (default is not None, f"DEFAULT ({default_prep!r})"),
                (primary, "PRIMARY KEY"),
                self.__conflict(notnull, "NOT NULL"),
                (generate, f"AS {generate}"),
                (store, "STORED"),
            ]
            if condition
        )

    def __set_name__(self, owner: T, name):
        if self.__name is None:
            self.__create = f"{name} {self.__create}"
        self.__table = owner
        self.__name = name

    def __get__(self, obj, t=None) -> "Field[T,F]" | F:
        if obj is None:
            return self  # called on type

        # called on instance
        name = f"__{self.__name}"
        v = getattr(obj, name, self.__default)
        if issubclass(self.__type, Model) and isinstance(v, int):
            v = (self.__type.id == v).get()
            setattr(obj, name, v)
        return v

    def __set__(self, obj, value):
        return setattr(obj, f"__{self.__name}", value)

    def __conflict(self, v, prefix):
        if not v or v == do.nothing:
            return False, ""
        if isinstance(v, conflict):
            return True, f"{prefix} ON CONFLICT {v}"
        return True, prefix

    def __eq__(self, __o: object) -> Query[T]:
        return Query(self.__table, where=Filter(self, cmp.eq, __o))

    def __lt__(self, __o: object) -> Query[T]:
        return Query(self.__table, where=Filter(self, cmp.lt, __o))

    def __gt__(self, __o: object) -> Query[T]:
        return Query(self.__table, where=Filter(self, cmp.gt, __o))

    def __ge__(self, __o: object) -> Query[T]:
        return Query(self.__table, where=Filter(self, cmp.ge, __o))

    def __le__(self, __o: object) -> Query[T]:
        return Query(self.__table, where=Filter(self, cmp.le, __o))

    def __ne__(self, __o: object) -> Query[T]:
        return Query(self.__table, where=Filter(self, cmp.ne, __o))

    def __pos__(self) -> Query[T]:
        return Query(self.__table, columns=self)

    def __and__(self, __o) -> Query[T]:
        return Query(self.__table, where=Filter(self, cmp.in_, +__o))

    def __str__(self):
        return self.__name

    def __repr__(self):
        return f"{SQL(self.__table)}.{self.__name}"

    def __sql__(self):
        return f"{SQL(self.__table)}.{self.__name}"

    def __params__(self):
        return ()

    def __create__(self):
        if self.__name is None:
            return ValueError("field not yet named")
        return self.__create

    def __default__(self) -> F:
        return self.__default

    ## AGGREGATIONS

    def avg(self, by=None):
        return Query(self.__table, columns=Agg("AVG", self, by=by))

    def count(self, by=None):
        return Query(self.__table, columns=Agg("COUNT", self, by=by))

    def sum(self, by=None):
        return Query(self.__table, columns=Agg("SUM", self, by=by))

    def min(self, by=None):
        return Query(self.__table, columns=Agg("MIN", self, by=by))

    def max(self, by=None):
        return Query(self.__table, columns=Agg("MAX", self, by=by))

    def total(self, by=None):
        return Query(self.__table, columns=Agg("TOTAL", self, by=by))


class Agg:
    def __init__(self, name, field=None, by=None) -> None:
        self.name = name
        self.field = field
        self.by = by
        pass

    def __sql__(self):
        return f"{self.name}({'*' if self.field is None else SQL(self.field)})"

    def __params__(self):
        if self.field is None:
            return ()
        return params(self.field)


class Connection(sqlite3.Connection):
    def logexec(self, f, args):
        try:
            return f(*args)
        except Exception as e:
            query, *params = args
            log.error(
                f"Failed to execute query {query!r} with parameters {params[0]!r}"
                if params
                else f"Failed to execute query {query!r}"
            )
            raise e

    def execute(self, *args):
        return self.logexec(super().execute, args)

    def executemany(self, *args):
        return self.logexec(super().executemany, args)

    def executescript(self, *args):
        return self.logexec(super().executescript, args)


class model_meta(QueryAPI, ABCMeta):
    _connection: Connection = None
    __fields__: tuple[str, ...] = ()

    def __new__(cls, name, bases, dict):
        # make sure all fields are in the current class dict, and are distinct instances from the superclass
        # so that __set_name__ works right
        for base in bases:
            dict.update(
                {
                    k: copy.copy(v)
                    for k, v in base.__dict__.items()
                    if k not in dict and isinstance(v, Field)
                }
            )
        dict["__fields__"] = tuple(k for k, v in dict.items() if isinstance(v, Field))

        return super().__new__(cls, name, bases, dict)

    def __sql__(cls):
        return cls.__name__.lower()

    def __params__(cls):
        return ()

    def count(self, by=None):
        return Query(self, columns=Agg("COUNT", by=by))

    def __len__(self):
        return self.count().get()

    def __pos__(self):
        return Query(self)

    def __getitem__(self, __o):
        return (+self)[__o]

    def __call__(self, *args, **kwargs):
        """This is non-standard for the QueryAPI, but it returns a new object."""
        return ABCMeta.__call__(self, *args, **kwargs)

    def __create__(cls):
        fields = (getattr(cls, f).__create__() for f in cls.__fields__)
        return f"CREATE TABLE {SQL(cls)} ({', '.join(fields)})"

    def get_or_create(cls, defaults: dict | None = None, **kwargs):
        if defaults is None:
            defaults = {}
        defaults.update(kwargs)
        try:
            return Query(
                cls,
                where=tuple(
                    Filter(getattr(cls, k), cmp.eq, v) for k, v in kwargs.items()
                ),
            ).get()
        except ValueError:
            return cls(**defaults).save()


class Model(QueryAPI, Reference, metaclass=model_meta):
    __all__ = set()
    __fields__: tuple[str, ...] = ()
    _connection: Connection = None
    id = Field(int, primary=True, notnull=True)

    def __init__(self, **kwargs) -> None:
        for name, value in kwargs.items():
            setattr(self, name, value)

    def __init_subclass__(cls):
        Model.__all__.add(cls)
        registerType(cls, lambda m: m.id, lambda b: int(b))

    def __pos__(self):
        return Query(self)

    @property
    def __values(self):
        return {f: getattr(self, f) for f in self.__fields__}

    def save(self):
        t = type(self)
        table = SQL(t)
        if self.id is None:
            sql = f"INSERT INTO {table} VALUES ({', '.join(f':{x}' for x in self.__fields__)}) "
        else:
            # TODO do foreign key recursion
            sql = (
                f"INSERT INTO {table} VALUES ({', '.join(f':{x}' for x in self.__fields__)}) "
                f"ON CONFLICT(id) DO UPDATE SET {', '.join(f'{x}=:{x}' for x in self.__fields__)} WHERE id=:id"
            )
        with self._connection as conn:
            self.id = conn.execute(sql, self.__values).lastrowid
        return self

    def delete(self):
        if self.id is None:
            return self
        with self._connection as conn:
            conn.execute(f"DELETE FROM {type(self)} WHERE id = {SQL(self.id)}")
        return self

    def copy(self):
        return type(self)(**self.__values, id=None)

    def __eq__(self, __o):
        # don't use isinstance here, because we don't want subclasses, only the exact class match
        if type(self) == type(__o):
            return self.id == __o.id
        return False


class sqlite_master(Model):
    id = None  # don't include id field
    type = Field(str)
    name = Field(str)
    tbl_name = Field(str)
    rootpage = Field(int)
    sql = Field(str)


def initialize_database(
    database="file::memory:?cache=shared",
    debug=False,
    allow_migrations=False,
    **options,
):
    """
    Allow the database to be used.

    This should be called after all models are defined and before any calls to the database are made.
    """

    options = {
        "database": database,
        "detect_types": sqlite3.PARSE_DECLTYPES,
        "uri": True,
        "factory": Connection,
        **options,
    }

    def connect(model=None):
        c = sqlite3.connect(**options)
        c.execute("PRAGMA FOREIGN_KEY=1")
        if model is not None:

            def factory(_, r):
                return model(**dict(zip(model.__fields__, r)))

            factory.__name__ = f"{model}_factory"
            c.row_factory = factory
        if debug:
            c.set_trace_callback(lambda msg: log.info(f"sql {msg!r}"))
        return c

    def execute(sql, params=()):
        with connect() as conn:
            return conn.execute(sql, params)

    conn = connect()
    Model._connection = connect()
    all_models = {}
    duplicates = {}
    for model in Model.__all__:
        name = SQL(model)
        # models that start with '_' are considered abstract
        # pseudo-models don't have an id
        if name.startswith("_") or model.id is None:
            continue
        model._connection = connect(model)
        if name in all_models:
            duplicates.setdefault(name, []).append(model.__module__)
        all_models[name] = model
    if duplicates:
        raise MigrationError(
            "Duplicate model found:\n"
            + "\n".join(f"{name:>16} in {info}" for name, info in duplicates.items())
        )

    # migrate database
    extant_tables = dict(
        (sqlite_master.type == "table")(sqlite_master.name, sqlite_master.sql)
    )
    migrations = {}
    for name, model in all_models.items():
        create_stmt = model.__create__()
        if name not in extant_tables:
            conn.execute(create_stmt, params(model))
            log.debug(f"made table: {name}")
        elif create_stmt == extant_tables[name]:
            log.debug(f"table {name} ok")
        else:
            old = set(
                chain.from_iterable(
                    conn.execute(f"select name from pragma_table_info('{SQL(model)}')")
                )
            )
            new = set(model.__fields__)
            shared = old.intersection(new)
            j = ", ".join
            migrations[name] = f"+({j(new - old)}) -({j(old - new)})"
            if not allow_migrations:
                continue
            fields = j(shared)
            conn.executescript(
                f"""
BEGIN;
PRAGMA FOREIGN_KEY = 0;
DROP TABLE IF EXISTS _{name};
ALTER TABLE {name} RENAME TO _{name};
{create_stmt};
INSERT INTO {name}({fields}) SELECT {fields} FROM _{name};
DROP TABLE _{name};
PRAGMA FOREIGN_KEY = 1;
COMMIT;
"""
            )

    if migrations:
        msg = "\n".join(f"{name:>16}: {info}" for name, info in migrations.items())
        if not allow_migrations:
            raise MigrationError("Migrations needed, but not allowed:\n" + msg)
        else:
            conn.execute("VACUUM")
            log.info("Migrations performed:\n" + msg)
    else:
        log.debug(f"database {database} ok")
    return execute


registerType(
    datetime.date,
    datetime.date.isoformat,
    lambda d: datetime.date.fromisoformat(d.decode()),
)
registerType(
    datetime.datetime,
    datetime.datetime.isoformat,
    lambda dt: datetime.datetime.fromisoformat(dt.decode()),
)
registerType(
    datetime.timedelta,
    datetime.timedelta.total_seconds,
    lambda td: datetime.timedelta(seconds=int(td)),
)
