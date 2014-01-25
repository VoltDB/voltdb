#!/usr/bin/python

import unittest
import schemaobject

class TestDatabaseSchema(unittest.TestCase):
    def setUp(self):
        self.db = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.db = self.db.selected

    def test_database_name(self):
        self.assertEqual("sakila", self.db.name)

    def test_database_option_charset(self):
        self.assertEqual("latin1", self.db.options['charset'].value)

    def test_database_option_collation(self):
        self.assertEqual("latin1_swedish_ci", self.db.options['collation'].value)

    def test_database_alter(self):
        self.assertEqual("ALTER DATABASE `sakila`", self.db.alter())

    def test_database_create(self):
        self.assertEqual("CREATE DATABASE `sakila` CHARACTER SET=latin1 COLLATE=latin1_swedish_ci;",
                          self.db.create())

    def test_database_drop(self):
        self.assertEqual("DROP DATABASE `sakila`;", self.db.drop())

    def test_databse_select(self):
        self.assertEqual("USE `sakila`;", self.db.select())

    def test_databses_eq(self):
        self.assertEqual(self.db, self.db)

    def test_databases_neq(self):
        self.assertNotEqual(self.db, None)

if __name__ == "__main__":
    from test_all import get_database_url
    TestDatabaseSchema.database_url = get_database_url()
    unittest.main()