#!/usr/bin/env pytest
from .microlite import *


class LibTest(TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)

        class Artist(Model):
            first_name = last_name = Field(str, "NA")
            birthday = Field(sqlite3.Date, sqlite3.Date(1000, 1, 1), not_null=True)

            @property
            def full_name(self):
                return f"{self.first_name} {self.last_name}"

        class Album(Model):
            artist = Field(Artist, not_null=True)
            title = Field(str, not_null=True)

        cls.artist = Artist
        cls.album = Album

    def test_render(self):
        # field
        self.assertEqual("S", str(Field(str, name="S")))
        self.assertEqual(
            "name TEXT DEFAULT (3) NOT NULL",
            repr(Field(str, name="name", default=3, not_null=True)),
        )
        # table
        self.assertEqual(
            f"CREATE TABLE {self.artist} ("
            "first_name TEXT DEFAULT ('NA'), "
            "last_name TEXT DEFAULT ('NA'), "
            "birthday DATE DEFAULT ('1000-01-01') NOT NULL, "
            "id INTEGER PRIMARY KEY NOT NULL)",
            repr(self.artist),
        )
        connect = self.initDatabase()
        # test default values
        omaewa = (
            connect()
            .execute("insert into artist(last_name) values ('Ni')")
            .execute("select * from artist where last_name ='Ni'")
            .fetchone()
        )
        self.assertEqual("NA", omaewa.first_name, msg="moushindeiru")

        #
        self.assertEqual(
            "SELECT * FROM artist WHERE birthday = '1000-01-01'",
            repr(self.artist(birthday=self.artist.birthday.default)),
        )

    def test_query(self):
        first_name = "Mario"
        last_name = "Peach"
        other_name = "other"
        self.initDatabase()
        self.artist.row(first_name, last_name).save()
        other = self.artist.row(other_name, other_name).save()

        # select
        self.assertEqual(
            list(self.artist.get(id=1)),
            [first_name, last_name, sqlite3.Date(year=1000, month=1, day=1), 1],
        )

        # update
        new_birthday = sqlite3.Date(2020, 1, 1)
        self.assertEqual(
            1, self.artist(first_name=first_name).update(birthday=new_birthday)
        )

        self.assertEqual(
            list(self.artist.get(id=1)),
            [first_name, last_name, new_birthday, 1],
        )
        self.assertNotEqual(
            self.artist.get(first_name=other_name).birthday, new_birthday
        )

        # delete
        self.assertEqual(1, self.artist.delete(last_name=last_name))
        self.assertEqual(self.artist.all(), [other])

    def test_row(self):
        self.initDatabase()
        r = self.artist.row("Mike", "Goldblum")
        self.assertEqual("Mike Goldblum", r.full_name)

        # insert
        self.assertEqual(r.id, None)
        r.save()
        self.assertEqual(r.id, 1)

        # update
        r.first_name = "Jeff"
        r.save()
        self.assertEqual(r.id, 1)
        self.assertEqual(len(self.artist().all()), 1)
        self.assertEqual(self.artist().first().first_name, "Jeff")

        r2 = self.artist.row("Do", "Little")
        r2.save()
        self.assertEqual(r2.id, 2)

        # delete
        r2.delete()
        self.assertEqual(r2.id, None)
        self.assertEqual(len(self.artist().all()), 1)
        self.assertEqual(self.artist().first().first_name, "Jeff")

    def test_foreign_key(self):
        db = self.initDatabase()
        artist = self.artist.row("Doja", "Cat").save()
        album = self.album.row(artist.id, "Hot Pink").save()
        self.assertEqual(album.artist.id, artist.id)
        self.assertEqual(album.artist.first_name, "Doja")

    def test_lookups(self):
        db = self.initDatabase()

        doja = self.artist.row("Doja", "Cat").save()
        hot_pink = self.album.row(doja, "Hot Pink").save()

        bd = sqlite3.Date(1995, 10, 21)
        mushroom = self.artist.row("Infected", "Mushroom", bd).save()
        nasa = self.album.row(mushroom, "Head of NASA and the two Amish boys").save()
        shawarma = self.album.row(mushroom, "The Legend of the Black Shawarma").save()
        self.assertListEqual(
            [nasa, shawarma],
            self.album(artist=mushroom).all(),
        )
        self.assertEqual(
            [hot_pink],
            self.album(artist__birthday__ne=bd).all(),
        )
        # TODO test get_or_create default handling

    @unittest.skip("Not implemented")
    def test_dirty_check(self):
        # TODO track if the row is dirty, and do a recursive save over foreign keys
        pass

    def test_custom_type(self):
        class newType(Type):
            def __init__(self, value):
                self.value = value

            @classmethod
            def from_sql(cls, b):
                return cls(b.decode("utf-8"))

            def to_sql(self):
                return str(self.value)

        class Newt(Model):
            field = Field(newType)

        self.initDatabase()
        val = "Eye"
        Newt.row(newType(val)).save()
        self.assertEqual(
            val,
            Newt.first().field.value,
        )

    def test_init(self):
        import gc

        class X(Model):
            original_field = Field(int)

        self.initDatabase()
        saved_id = X.row().save().id
        self.assertEqual(
            ["original_field", "id"],
            list(map(str, X._fields)),
        )

        class X(Model):
            new_field = Field(int)

        with self.assertRaises(
            ImportError, msg="Should not tolerate duplicate model definitions."
        ):
            self.initDatabase()

        gc.collect()  # clean up the first X, which has no referees now

        with self.assertRaises(
            EnvironmentError,
            msg="Should not allow migrations if allow_migrations=False",
        ):
            self.initDatabase()

        initialize_database(self.db, True, allow_migrations=True)
        self.assertEqual(
            ["new_field", "id"],
            list(map(str, X._fields)),
        )

        # this row should have been copied over when the table was migrated
        X.get(id=saved_id)

        # TODO show that migrations fail and roll back on foreign key constraint failure


if __name__ == "__main__":
    unittest.main()
