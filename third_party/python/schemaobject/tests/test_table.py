#!/usr/bin/python

import unittest
import schemaobject

class TestTableSchema(unittest.TestCase):
    def setUp(self):
        self.db = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.db = self.db.selected

    def test_table_count(self):
        self.assertEqual(16, len(self.db.tables))

    def test_tables(self):
        self.assertEqual(self.db.tables.keys(), ['actor','address','category','city','country','customer',
         'film','film_actor','film_category','film_text','inventory',
          'language','payment','rental','staff','store'])

    def test_table_name(self):
        self.assertEqual("address", self.db.tables['address'].name)

    def test_table_option_engine(self):
        self.assertEqual("InnoDB", self.db.tables['address'].options['engine'].value)

    def test_table_option_charset(self):
        self.assertEqual("utf8", self.db.tables['address'].options['charset'].value)

    def test_table_option_collation(self):
        self.assertEqual("utf8_general_ci", self.db.tables['address'].options['collation'].value)

    def test_table_option_row_format(self):
        self.assertEqual("Compact", self.db.tables['address'].options['row_format'].value)

    def test_table__option_create_options(self):
        self.assertEqual("", self.db.tables['actor'].options['create_options'].value)

    def test_table_option_auto_increment(self):
        self.assertEqual(1, self.db.tables['actor'].options['auto_increment'].value)

    def test_table_option_comment(self):
        self.assertEqual("", self.db.tables['address'].options['comment'].value)

    def test_table_column_count(self):
        self.assertEqual(8, len(self.db.tables['address'].columns))

    def test_table_index_count(self):
        self.assertEqual(2, len(self.db.tables['address'].indexes))

    def test_table_fk_count(self):
        self.assertEqual(1, len(self.db.tables['address'].foreign_keys))

    def test_tables_eq(self):
        self.assertEqual(self.db.tables['address'], self.db.tables['address'])

    def test_tables_neq(self):
        self.assertNotEqual(self.db.tables['address'], self.db.tables['actor'])

    def test_table_alter(self):
        self.assertEqual("ALTER TABLE `address`", self.db.tables['address'].alter())

    def test_table_create(self):
        stub = 'CREATE TABLE `actor` ( `actor_id` smallint(5) unsigned NOT NULL AUTO_INCREMENT, `first_name` varchar(45) NOT NULL, `last_name` varchar(45) NOT NULL, `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`actor_id`), KEY `idx_actor_last_name` (`last_name`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;'
        self.assertEqual(stub, self.db.tables['actor'].create())

    def test_table_drop(self):
        self.assertEqual("DROP TABLE `actor`;", self.db.tables['actor'].drop())

if __name__ == "__main__":
    from test_all import get_database_url
    TestTableSchema.database_url = get_database_url()
    unittest.main()