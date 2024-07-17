import gc
import logging
import datetime
import unittest
import pytest
import sqlite3
import microlite as m


class TestCase(unittest.TestCase):
    db = "file::memory:?cache=shared"

    def setUp(self):
        """Drop all tables before each test"""
        gc.collect()  # make sure we don't have any old references hanging around to lock the database
        with sqlite3.connect(self.db, uri=True) as conn:
            for r in conn.execute(
                "select name from sqlite_master where type='table'"
            ).fetchall():
                conn.execute(f"drop table {r[0]}")
        m.Model.__all__.clear()

    def initDB(self, migrate=False):
        return m.initialize_database(self.db, debug=True, allow_migrations=migrate)

    def fillDB(self):
        class Collector(m.Model):
            name = m.Field(str, "NA")

        self.collector = Collector

        class Artist(m.Model):
            name = m.Field(str, "NA", notnull=True)
            birthday = m.Field(datetime.date, datetime.date(1000, 1, 1), notnull=True)

        m.registerType(Artist, lambda m: m.id, lambda b: int(b))
        self.artist = Artist

        class Painting(m.Model):
            name = m.Field(str)
            artist = m.Field(Artist)
            list_price = m.Field(float)

            def __str__(self):
                return f"{self.name!r} by {self.artist} worth {self.list_price}"

        self.painting = Painting

        class Sale(m.Model):
            date = m.Field(datetime.date)
            painting = m.Field(Painting)
            artist = m.Field(Artist)
            collector = m.Field(Collector)
            price = m.Field(float)

        self.sale = Sale

        execute = self.initDB()

        a = Artist(name="Abe").save()
        a1 = Painting(name="steak", artist=a, list_price=1.0).save()

        b = Artist(name="Betty").save()
        b1 = Painting(name="boop", artist=b, list_price=1.0).save()
        b2 = Painting(name="Sailorman", artist=b, list_price=2.0).save()

        c = Collector(name="Carol").save()
        d = Collector(name="Dan").save()

        Sale(painting=a1, artist=a, collector=c, price=1.0).save()
        Sale(painting=b1, artist=b, collector=c, price=1.0).save()
        Sale(painting=b2, artist=b, collector=d, price=1.0).save()

        return execute

    def testRender(self):
        # field
        f = m.Field(int, 3, notnull=m.conflict.rollback)
        # __set_name__ is usually called implicitly when the class is defined
        f.__set_name__("fake_table", "field_name")
        self.assertEqual(
            "field_name INTEGER DEFAULT (3) NOT NULL ON CONFLICT ROLLBACK",
            f.__create__(),
        )
        _ = self.fillDB()
        # table
        self.assertEqual(
            "CREATE TABLE artist (name TEXT DEFAULT ('NA') NOT NULL, "
            "birthday date DEFAULT ('1000-01-01') NOT NULL, "
            "id INTEGER PRIMARY KEY NOT NULL)",
            self.artist.__create__(),
        )

        self.assertEqual(
            "(SELECT * FROM artist WHERE artist.birthday = (?))",
            m.SQL(self.artist.birthday == datetime.date(2000, 1, 1)),
        )

    def testQueryGet(self):
        _ = self.fillDB()
        self.assertEqual(1, (self.artist.name == "Abe").get().id)
        self.assertRaises(m.DoesNotExist, (self.artist.name == "A.I.").get)

        # insert a new artist and show it can be retrieved
        n = "Edward"
        ed = self.artist(name=n).save()
        self.assertEqual(ed, (self.artist.name == n).get(), msg="query select")

        self.assertEqual(ed, self.artist.get_or_create(name=n))

    def testQueryDeep(self):
        # specify that a foreign key relationship should be fetched all in a single query
        _ = self.fillDB()
        s = self.sale.first()
        self.assertIsInstance(
            s.artist,
            self.artist,
            msg="by referencing a foreign key we should have fetched the object",
        )

    def testQueryDelete(self):
        _ = self.fillDB()
        a = "Abe"
        qa = self.artist.name == a
        self.assertEqual(a, qa.get().name)
        self.assertEqual(1, qa.delete())
        self.assertRaises(m.DoesNotExist, qa.get)

    def testQueryOrder(self):
        _ = self.fillDB()
        self.assertEqual(
            "(SELECT artist.name FROM artist ORDER BY artist.id)",
            m.SQL(self.artist.name.sort(self.artist.id)),
        )

    @unittest.skip("not done yet")
    def testJoin(self):
        self.fail("inner join, outer join, join by foreign key")

    def testQueryDistinct(self):
        _ = self.fillDB()
        n = "pete"
        self.artist(name=n).save()
        self.artist(name=n).save()
        self.artist(name=n).save()
        self.assertEqual(3, len(self.artist.name == n))
        self.assertEqual(
            "(SELECT DISTINCT artist.name FROM artist)",
            m.SQL((+self.artist.name)(distinct=True)),
        )

    def testLimit(self):
        _ = self.fillDB()
        self.assertEqual(
            "(SELECT collector.name FROM collector WHERE collector.id IN (SELECT sale.collector FROM sale) LIMIT 5)",
            m.SQL((self.collector.id & self.sale.collector)[:5](self.collector.name)),
        )

    @unittest.skip("uncertain about the syntax")
    def testAgg(self):
        _ = self.fillDB()
        self.assertEqual(
            "(SELECT * FROM painting WHERE painting.list_price > (SELECT AVG(painting.list_price) FROM painting))",
            m.SQL(self.painting.list_price > self.painting.list_price.avg()),
        )
        self.assertEqual(
            "(SELECT collector.name, (SELECT COUNT(*) FROM sale WHERE collectors.id = sales.collector) FROM collectors)",
            m.SQL(
                self.collector.name(
                    self.sale.count()[self.collector.id == self.sale.collector]
                )
            ),
        )

    def testConform(self):
        """
        ABCMeta ensures that all subclasses of QueryAPI implement its abstract methods,
        but the non-abstract methods defer to Query, so Query must override all methods of QueryAPI,
        not just the abstract methods.
        """
        api = (
            func for func in m.QueryAPI.__dict__ if callable(getattr(m.QueryAPI, func))
        )
        impl = tuple(k for k in m.Query.__dict__.keys() if not k.startswith("_Query__"))
        for f in api:
            with self.subTest(function=f):
                self.assertIn(f, impl)

    def testMigrations(self):
        class X(m.Model):
            original_field = m.Field(int)

        self.initDB()
        saved_id = X(original_field=0).save().id
        self.assertEqual(
            ("original_field", "id"),
            X.__fields__,
        )

        class X(m.Model):
            new_field = m.Field(int)

        with self.assertRaises(
            m.MigrationError, msg="Should not tolerate duplicate model definitions."
        ):
            self.initDB()

        m.Model.__all__.clear()
        m.Model.__all__.add(X)
        gc.collect()  # clean up the first X, which has no referees now

        with self.assertRaises(
            m.MigrationError,
            msg="Should not allow migrations if allow_migrations=False",
        ):
            self.initDB()

        self.initDB(migrate=True)
        self.assertEqual(
            ("new_field", "id"),
            X.__fields__,
        )

        # this row should have been copied over when the table was migrated
        (X.id == saved_id).get()

        # TODO show that migrations fail and roll back on foreign key constraint failure


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import pytest

    pytest.main([__file__])
