#!/usr/bin/env python3
from microlite import *


class LibTest(TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.INFO)

        class Artist(Model):
            first_name = last_name = Field(str, "NA")
            birthday = Field(sqlite3.Date, sqlite3.Date(1000, 1, 1), not_null=True)

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
        omaewa = (
            connect()
            .execute("insert into artist(last_name) values ('Ni')")
            .execute("select * from artist where last_name ='Ni'")
            .fetchone()
        )
        self.assertEqual("NA", omaewa.first_name, msg="moushindeiru")

    def test_select(self):
        first_name = "Mario"
        last_name = "Peach"
        self.initDatabase()
        self.artist.row(first_name, last_name).save()
        self.assertEqual(
            list(self.artist.get(id=1)),
            [first_name, last_name, sqlite3.Date(year=1000, month=1, day=1), 1],
        )

    def test_row(self):
        self.initDatabase()
        r = self.artist.row("Mike", "Goldblum")

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
        album = self.album.row(artist, "Hot Pink").save()
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

    @unittest.skip(NotImplemented)
    def test_dirty_check(self):
        # track if the row is dirty, and do a recursive save over foreign keys
        pass

    @unittest.skip(NotImplemented)
    def test_slice(self):
        """
        slicing a query:
        * int -> offset n
        * slice -> limit stop - start offset start (disallow step)
        """
        pass

    @unittest.skip(NotImplemented)
    def test_migrations(self):
        """
        show that migrations:
        * only run when allow_migrations=True
        * preserve as much data as they can
        * fail and roll back on foreign key constraint failure
        """


if __name__ == "__main__":
    unittest.main()
