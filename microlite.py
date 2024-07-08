import abc
import logging
import datetime
import sqlite3

assert sqlite3.sqlite_version_info >= (
    3,
    24,
), "Your database engine (not the python package) is out of date."

log = logging.getLogger(__name__)

# Foreign key flags
Restrict = "restrict"
SetNull = "set null"
SetDefault = "set default"
Cascade = "cascade"


class Connection(sqlite3.Connection):
    class Cursor(sqlite3.Cursor):
        def logexec(self, f, args):
            try:
                return f(*args)
            except Exception as e:
                query, *params = args
                log.error(
                    f"Failed to execute query {query!r}{f' with parameters {params[0]!r}' if params else ''}."
                )
                raise e

    for f in ("execute", "executemany", "executescript"):
        setattr(
            Cursor,
            f,
            (lambda f: lambda s, *a: s.logexec(getattr(super(type(s), s), f), a))(f),
        )

    def cursor(self, factory=Cursor):
        return super().cursor(factory)


def initialize_database(
    database="file::memory:?cache=shared",
    debug=False,
    allow_migrations=False,
    **options,
):
    """
    Allow the databse to be used.

    This should be called after all models are defined and before any calls to the database are made.
    """

    options = {
        "database": database,
        "detect_types": sqlite3.PARSE_DECLTYPES,
        "uri": True,
        "factory": sqlite3.Connection,
        **options,
    }

    keep_alive = sqlite3.connect(**options) if "memory" in database else None

    def connect(model=None):
        keep_alive  # capture value in closure
        c = sqlite3.connect(**options)
        c.execute("PRAGMA FOREIGN_KEY=1")
        c.row_factory = model and (lambda _, r: model.row(*r)) or Row
        if debug:
            c.set_trace_callback(log.debug)
        return c

    # allow connections
    Model._connect = connect

    def all_subclasses(cls):
        subs = {*cls.__subclasses__()}
        return subs.union({c for s in subs for c in all_subclasses(s)})

    with connect() as conn:
        # register models
        all_models = {}
        duplicates = {}
        for model in all_subclasses(Model):
            name = str(model)
            # models that start with '_' are considered abstract
            # pseudo-models don't have an id
            if name.startswith("_") or model.id is None:
                continue
            if name in all_models:
                duplicates.setdefault(name, []).append(model.__module__)
            all_models[name] = model
        if duplicates:
            raise ImportError(
                "Duplicate model found:\n"
                + "\n".join(
                    f"{name:>16} in {info}" for name, info in duplicates.items()
                )
            )

        # migrate database
        extant_tables = dict(sqlite_master(type="table")["name", "sql"])
        migrations = {}
        for name, model in all_models.items():
            create_stmt = repr(model)
            if name not in extant_tables:
                # print(create_stmt, model._defaults)
                conn.execute(create_stmt)
            elif create_stmt == extant_tables[name]:
                log.debug(f"table {name} ok")
            else:
                old = set(field.name for field in table_info(table=name))
                new = set(map(str, model._fields))
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
            raise EnvironmentError("Migrations needed, but not allowed:\n" + msg)
        else:
            conn.execute("VACUUM")
            log.info("Migrations performed:\n" + msg)
    else:
        log.debug(f"database {database} ok")
    return connect


def clean_dict(d):
    """Return an object suitable for saving."""
    return {k: v["id"] if isinstance(v, _query.row) else v for k, v in d.items()}


class Field:
    def __init__(
        self,
        type: type,
        default=None,
        primary_key=False,
        not_null=False,
        on_delete="",
        on_update="",
        generate="",
        stored=False,
        name="",
    ):
        self.name = name
        self.type = type
        self.on_delete = on_delete
        self.on_update = on_update
        self.default = default
        self.primary_key = primary_key
        self.not_null = not_null
        self.generate = generate
        self.stored = stored

    def __str__(self):
        return self.name

    def __repr__(self):
        typename = (
            f"INTEGER REFERENCES {self.type}"
            if issubclass(self.type, Model)
            else {
                type(None): "NULL",
                int: "INTEGER",
                float: "REAL",
                str: "TEXT",
                bytes: "BLOB",
            }.get(self.type, self.type.__name__)
        )
        default = sqlite3.adapters.get(
            (type(self.default), sqlite3.PrepareProtocol), lambda x: x
        )(self.default)

        return " ".join(
            value
            for condition, value in [
                (True, self.name),
                (True, typename),
                (self.on_delete, f"ON DELETE {self.on_delete}"),
                (self.on_update, f"ON UPDATE {self.on_update}"),
                (self.default is not None, f"DEFAULT ({default!r})"),
                (self.primary_key, "PRIMARY KEY"),
                (self.not_null, "NOT NULL"),
                (self.generate, f"AS {self.generate}"),
                (self.stored, "STORED"),
            ]
            if condition
        )


class Row(sqlite3.Row, abc.ABC):
    def __getattr__(self, item):
        return self[item]

    def __repr__(self):
        return repr((*self,))


class _allow_bare:
    def __init__(self, func):
        self.__func = func

    def __get__(self, obj, cls):
        return lambda *fields, **filters: self.__func(
            cls(*fields, **filters)
            if obj is None
            else (
                cls(*obj._fields, *fields, **obj._filters, **filters)
                if fields or filters
                else obj
            )
        )


class _query:
    def _connect(model=None) -> sqlite3.Connection:
        raise Exception("database not initialized")

    def __init__(self, *fields, **filters):
        self._fields = fields
        self._filters = filters

    def __getitem__(self, item):
        if not isinstance(item, tuple):
            item = (item,)
        return type(self)(*self._fields, *map(str, item), **self._filters)

    def __call__(self, **filters):
        return type(self)(*self._fields, **self._filters, **filters)

    def __repr__(self):
        params = (
            self._connect()
            .execute(
                f"SELECT {', '.join('?' * len(self._filters))}",
                tuple(clean_dict(self._filters).values()),
            )
            .fetchone()
        )
        query, items = self._select, sorted(zip(self._filters, params), reverse=True)
        for k, v in items:
            query = query.replace(f":{k}", repr(v))
        return query

    def _execute(self, query):
        with type(self)._connect() if self._fields else self._connect() as conn:
            return conn.execute(query, clean_dict(self._filters))

    def __iter__(self):
        return self._execute(self._select)

    @_allow_bare
    def count(self):
        return (
            self._connect()
            .execute(
                f"SELECT COUNT(*) FROM {type(self)} WHERE {self._where}",
                clean_dict(self._filters),
            )
            .fetchone()[0]
        )

    @property
    def _where(self):
        cmp = {
            "eq": "=",
            "gt": ">",
            "lt": "<",
            "ge": ">=",
            "le": "<=",
            "ne": "<>",
            "in": "in",
        }
        clauses = []
        for filter, value in clean_dict(self._filters).items():
            clause = "{}"
            model = type(self)
            fields = filter.split("__")
            op = cmp[fields.pop()] if fields[-1] in cmp else "="
            for field in fields[:-1]:
                model = getattr(model, field).type
                clause = clause.format("{} IN (SELECT id FROM {} WHERE {})").format(
                    field, model, "{}"
                )
            clauses.append(clause.format(f"{fields[-1]} {op} :{filter}"))
        return " AND ".join(clauses) or 1

    @property
    def _select(self):
        return f"SELECT {', '.join(map(str, self._fields)) or '*'} FROM {type(self)} WHERE {self._where}"

    @_allow_bare
    def all(self):
        return list(self)

    @_allow_bare
    def first(self):
        return next(iter(self))

    @_allow_bare
    def get(self):
        items = iter(self).fetchmany(2)
        if len(items) != 1:
            raise ValueError(
                f"{['No', 'Multiple'][bool(items)]} objects returned by get."
            )
        return items[0]

    @_allow_bare
    def delete(self):
        return self._execute(f"DELETE FROM {type(self)} WHERE {self._where}").rowcount

    def update(self, **fields):
        where = self._where
        if fields:
            self = self(**fields)
        fields = "".join(f"{f}=:{f}" for f in fields)
        return self._execute(f"UPDATE {type(self)} SET {fields} WHERE {where}").rowcount

    @_allow_bare
    def create(self):
        return (
            type(self)
            .row(**{k: v for k, v in {**self._filters}.items() if "__" not in k})
            .save()
        )

    @_allow_bare
    def get_or_create(self):
        try:
            return self.get()
        except ValueError:
            return self.create()

    class row(dict):
        _model = None  # placeholder

        def __init__(self, *args, **kwargs):
            args = list(reversed(args))
            super().__init__(
                {
                    field.name: (
                        kwargs.pop(field.name)
                        if field.name in kwargs
                        else args.pop() if args else field.default
                    )
                    for field in self._model
                },
            )

        def __iter__(self):
            return iter(self.values())

        def __getattr__(self, item):
            value = self[item]
            # handle foreign keys
            fk = getattr(self._model, item)
            if isinstance(value, int) and issubclass(fk.type, Model):
                value = fk.type.get(id=value)
                self[item] = value
            return value

        def __setattr__(self, key, value):
            if key in self:
                self[key] = value

        def __eq__(self, other):
            # a row is also considered equal to its id
            return isinstance(other, int) and self.id == other or super().__eq__(other)

        def delete(self):
            if self.id:
                render = f"DELETE FROM {self._model} WHERE id = :id"
                with self._model._connect() as conn:
                    conn.execute(render, clean_dict(self))
                self.id = None
                return True
            return False

        def save(self):
            render = (
                f"INSERT INTO {self._model} VALUES ({', '.join(f':{f}' for f in self._model)}) "
                f"ON CONFLICT(id) DO UPDATE SET {', '.join(f'{f}=:{f}' for f in self._model)} WHERE id=:id"
            )
            with self._model._connect() as conn:
                self.id = conn.execute(render, clean_dict(self)).lastrowid or self.id
            return self

    Row.register(row)


class _model_meta(type):
    def __new__(mcs, table_name, query_bases, row_dict):
        row_dict.pop("__qualname__", None)
        row_dict.pop("__module__", None)
        super_query = row_dict.pop("Query", None)
        if super_query:
            query_bases = (super_query, *query_bases)
        query_dict = {
            k: Field(**{**row_dict.pop(k).__dict__, "name": k})
            for k in list(row_dict)
            if isinstance(row_dict[k], Field)
        }
        for base in query_bases:
            query_dict.update(
                {
                    k: v
                    for k, v in base.__dict__.items()
                    if k not in query_dict and isinstance(v, Field)
                }
            )
        query_dict.update({k: v for k, v in row_dict.items() if k in query_dict})
        query_dict["_fields"] = tuple(
            (v for v in query_dict.values() if isinstance(v, Field))
        )
        # query_dict["_defaults"] = tuple(f.default for f in query_dict["_fields"] if f.default is not None)

        query = super().__new__(mcs, table_name, query_bases, query_dict)
        query.row = type(f"{query}_row", (query.row,), {**row_dict, "_model": query})
        return query

    def __str__(cls):
        return cls.__name__.lower()

    def __iter__(cls):
        return iter(cls._fields)

    def __repr__(cls):
        return f"CREATE TABLE {cls} ({', '.join(map(repr, cls))})"


class Model(_query, metaclass=_model_meta):
    id = Field(int, primary_key=True, not_null=True)


class sqlite_master(Model):
    id = None  # don't include id field
    type = name = tbl_name = Field(str)
    rootpage = Field(int)
    sql = Field(str)


class table_info(Model):
    id = None  # don't include id field
    cid = Field(int)
    name = type = Field(str)
    notnull = Field(int)
    dflt_value = Field(bytes)
    pk = Field(int)

    class Query:
        @property
        def _select(self):
            return f"SELECT {', '.join(self._fields) or '*'} FROM pragma_table_info(:table)"


def registerType(type, to_sql, from_sql):
    sqlite3.register_adapter(type, to_sql)
    sqlite3.register_converter(type.__name__, from_sql)


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
