#!/usr/bin/python
import unittest
import schemaobject

class TestColumnSchema(unittest.TestCase):
    def setUp(self):
        self.db = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.db = self.db.selected

    def test_column_count(self):
        self.assertEqual(9, len(self.db.tables['customer'].columns))

    def test_columns(self):
        self.assertEqual(self.db.tables['customer'].columns.keys(),
                        ['customer_id', 'store_id', 'first_name', 'last_name',
                         'email', 'address_id', 'active', 'create_date', 'last_update'])

    def test_column_field(self):
        self.assertEqual("store_id", self.db.tables['customer'].columns['store_id'].field)

    def test_column_type(self):
        self.assertEqual("varchar(50)", self.db.tables['customer'].columns['email'].type)

    def test_column_type_enum(self):
        self.assertEqual("enum('G','PG','PG-13','R','NC-17')",
                         self.db.tables['film'].columns['rating'].type)

    def test_column_type_set(self):
        self.assertEqual("set('Trailers','Commentaries','Deleted Scenes','Behind the Scenes')",
                          self.db.tables['film'].columns['special_features'].type)

    def test_column_charset(self):
        self.assertEqual("utf8", self.db.tables['customer'].columns['last_name'].charset)

    def test_column_collation(self):
        self.assertEqual("utf8_general_ci", self.db.tables['customer'].columns['last_name'].collation)

    def test_column_null(self):
        self.assertTrue(self.db.tables['customer'].columns['email'].null)

    def test_column_not_null(self):
        self.assertFalse(self.db.tables['customer'].columns['active'].null)

    def test_column_key(self):
        self.assertEqual('PRI', self.db.tables['customer'].columns['customer_id'].key)

    def test_column_default(self):
        self.assertEqual("CURRENT_TIMESTAMP", self.db.tables['customer'].columns['last_update'].default)

    def test_column_extra(self):
        self.assertEqual('auto_increment', self.db.tables['customer'].columns['customer_id'].extra)

    def test_column_comment(self):
        self.assertEqual('', self.db.tables['customer'].columns['store_id'].comment)

    def test_columns_eq(self):
        self.assertEqual(self.db.tables['customer'].columns['store_id'],
                         self.db.tables['customer'].columns['store_id'])

    def test_columns_neq(self):
        self.assertNotEqual(self.db.tables['customer'].columns['store_id'],
                            self.db.tables['customer'].columns['last_name'])

    def test_column_null(self):
        self.assertEqual("`email` varchar(50) NULL AFTER `last_name`",
                        self.db.tables['customer'].columns['email'].define(after='last_name'))

    def test_column_not_null(self):
        self.db.tables['customer'].columns['email'].null = True
        self.assertEqual("`email` varchar(50) NULL AFTER `last_name`",
                        self.db.tables['customer'].columns['email'].define(after='last_name'))

    def test_column_default_string(self):
        self.db.tables['rental'].columns['rental_date'].default = '0000-00-00 00:00:00'
        self.assertEqual("`rental_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' AFTER `rental_id`",
                        self.db.tables['rental'].columns['rental_date'].define(after='rental_id'))

        self.db.tables['customer'].columns['email'].null = False
        self.db.tables['customer'].columns['email'].default = ''
        self.assertEqual("`email` varchar(50) NOT NULL DEFAULT '' AFTER `last_name`",
                        self.db.tables['customer'].columns['email'].define(after='last_name'))

    def test_column_default_number(self):
        self.db.tables['rental'].columns['customer_id'].default = 123
        self.assertEqual("`customer_id` smallint(5) unsigned NOT NULL DEFAULT 123 AFTER `inventory_id`",
                        self.db.tables['rental'].columns['customer_id'].define(after='inventory_id'))

        self.db.tables['rental'].columns['customer_id'].default = 0
        self.assertEqual("`customer_id` smallint(5) unsigned NOT NULL DEFAULT 0 AFTER `inventory_id`",
                        self.db.tables['rental'].columns['customer_id'].define(after='inventory_id'))

    def test_column_default_reserved(self):
        self.assertEqual("`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP AFTER `staff_id`",
                        self.db.tables['rental'].columns['last_update'].define(after='staff_id'))

    def test_column_no_default(self):
        self.assertEqual("`email` varchar(50) NULL AFTER `last_name`",
                        self.db.tables['customer'].columns['email'].define(after='last_name'))

    def test_column_extra(self):
        self.assertEqual("`customer_id` smallint(5) unsigned NOT NULL auto_increment FIRST",
                    self.db.tables['customer'].columns['customer_id'].define())

    def test_column_no_extra(self):
        self.assertEqual("`email` varchar(50) NULL AFTER `last_name`",
                         self.db.tables['customer'].columns['email'].define(after='last_name'))

    def test_column_comment(self):
        self.db.tables['customer'].columns['email'].comment = "email address field"
        self.assertEqual("`email` varchar(50) NULL COMMENT 'email address field' AFTER `last_name`",
                         self.db.tables['customer'].columns['email'].define(after='last_name', with_comment=True))

    def test_column_no_comment(self):
        self.db.tables['customer'].columns['email'].comment = "email address field"
        self.assertEqual("`email` varchar(50) NULL AFTER `last_name`",
                         self.db.tables['customer'].columns['email'].define(after='last_name', with_comment=False))

    def test_column_no_charset_collate(self):
        self.assertEqual("`customer_id` smallint(5) unsigned NOT NULL auto_increment FIRST",
                         self.db.tables['customer'].columns['customer_id'].define())

    def test_column_charset_collate_same_as_parent(self):
        self.assertEqual("`first_name` varchar(45) NOT NULL AFTER `store_id`",
                         self.db.tables['customer'].columns['first_name'].define(after='store_id'))

    def test_column_charset_collate_diff_from_parent(self):
        self.db.tables['customer'].columns['first_name'].charset = 'latin1'
        self.db.tables['customer'].columns['first_name'].collation = 'latin1_general_ci'

        self.assertEqual("`first_name` varchar(45) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL AFTER `store_id`",
                          self.db.tables['customer'].columns['first_name'].define(after='store_id'))

    def test_parent_charset_collate_diff_from_column(self):
        self.db.tables['customer'].options['charset'].value = 'latin1'
        self.db.tables['customer'].options['collation'].value = 'latin1_general_ci'

        self.assertEqual("`first_name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL AFTER `store_id`",
                         self.db.tables['customer'].columns['first_name'].define(after='store_id'))

    def test_column_after(self):
        self.assertEqual("`last_name` varchar(45) NOT NULL AFTER `first_name`",
                         self.db.tables['customer'].columns['last_name'].define(after='first_name'))

    def test_column_first(self):
        self.assertEqual("`last_name` varchar(45) NOT NULL FIRST",
                         self.db.tables['customer'].columns['last_name'].define(after=None))

    def test_column_definition_syntax(self):
        self.db.tables['customer'].columns['first_name'].default = "bob"
        self.db.tables['customer'].columns['first_name'].comment = "first name"
        self.db.tables['customer'].columns['first_name'].charset = 'latin1'
        self.db.tables['customer'].columns['first_name'].collation = 'latin1_general_ci'
        self.db.tables['customer'].columns['customer_id'].default = 0

        self.assertEqual("`first_name` varchar(45) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL DEFAULT 'bob' COMMENT 'first name' AFTER `store_id`",
                         self.db.tables['customer'].columns['first_name'].define(after='store_id', with_comment=True))

        self.assertEqual("`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP AFTER `staff_id`",
                         self.db.tables['rental'].columns['last_update'].define(after='staff_id'))

        self.assertEqual("`customer_id` smallint(5) unsigned NOT NULL DEFAULT 0 auto_increment FIRST",
                         self.db.tables['customer'].columns['customer_id'].define())

    def test_create_column(self):
        self.assertEqual("ADD COLUMN `last_name` varchar(45) NOT NULL AFTER `first_name`",
                         self.db.tables['customer'].columns['last_name'].create(after='first_name'))

    def test_create_column_with_comment(self):
        self.db.tables['customer'].columns['last_name'].comment = "hello"
        self.assertEqual("ADD COLUMN `last_name` varchar(45) NOT NULL COMMENT 'hello' AFTER `first_name`",
                      self.db.tables['customer'].columns['last_name'].create(after='first_name', with_comment=True))
        self.db.tables['customer'].columns['last_name'] = ''

    def test_modify_column(self):
        self.assertEqual("MODIFY COLUMN `last_name` varchar(45) NOT NULL AFTER `first_name`",
                         self.db.tables['customer'].columns['last_name'].modify(after='first_name'))

    def test_modify_column_with_comment(self):
        self.db.tables['customer'].columns['last_name'].comment = "hello"
        self.assertEqual("MODIFY COLUMN `last_name` varchar(45) NOT NULL COMMENT 'hello' AFTER `first_name`",
                      self.db.tables['customer'].columns['last_name'].modify(after='first_name', with_comment=True))
        self.db.tables['customer'].columns['last_name'] = ''

    def test_column_drop(self):
        self.assertEqual("DROP COLUMN `last_name`",
                         self.db.tables['customer'].columns['last_name'].drop())

if __name__ == "__main__":
    from test_all import get_database_url
    TestColumnSchema.database_url = get_database_url()
    unittest.main()