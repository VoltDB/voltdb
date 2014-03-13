#!/usr/bin/python
import re
import unittest
import schemaobject
from schemaobject.foreignkey import REGEX_FK_REFERENCE_OPTIONS

class TestForeignKeySchema(unittest.TestCase):
    def setUp(self):
        self.schema = schemaobject.SchemaObject(self.database_url + 'sakila')
        self.fk = self.schema.selected.tables['rental'].foreign_keys

    def test_FK_exists(self):
        self.assertTrue("fk_rental_customer" in self.fk.keys())

    def test_FK_not_exists(self):
        self.assertFalse("fk_foobar" in self.fk.keys())

    def test_FK_name(self):
        self.assertEqual("fk_rental_customer", self.fk['fk_rental_customer'].name)

    def test_FK_symbol(self):
        self.assertEqual("fk_rental_customer", self.fk['fk_rental_customer'].symbol)

    def test_FK_table_name(self):
        self.assertEqual("rental", self.fk['fk_rental_customer'].table_name)

    def test_FK_table_schema(self):
        self.assertEqual("sakila", self.fk['fk_rental_customer'].table_schema)

    def test_FK_columns(self):
        self.assertEqual(['customer_id'], self.fk['fk_rental_customer'].columns)

    def test_FK_referenced_table_name(self):
        self.assertEqual("customer", self.fk['fk_rental_customer'].referenced_table_name)

    def test_FK_referenced_table_schema(self):
        self.assertEqual("sakila", self.fk['fk_rental_customer'].referenced_table_schema)

    def test_FK_referenced_columns(self):
        self.assertEqual(['customer_id'], self.fk['fk_rental_customer'].referenced_columns)

    def test_FK_match_option(self):
        self.assertEqual(None, self.fk['fk_rental_customer'].match_option)

    def test_FK_update_rule(self):
        self.assertEqual("CASCADE", self.fk['fk_rental_customer'].update_rule)

    def test_FK_delete_rule(self):
        self.assertEqual("RESTRICT", self.fk['fk_rental_customer'].delete_rule)

    def test_format_referenced_col_with_length(self):
        self.assertEqual('`fk_rental_customer`(11)', schemaobject.foreignkey.ForeignKeySchema._format_referenced_col('fk_rental_customer', 11))

    def test_format_referenced_col_without_length(self):
        self.assertEqual('`fk_rental_customer`', schemaobject.foreignkey.ForeignKeySchema._format_referenced_col('fk_rental_customer', 0))
        self.assertEqual('`fk_rental_customer`', schemaobject.foreignkey.ForeignKeySchema._format_referenced_col('fk_rental_customer', None))

    def test_FK_drop(self):
        self.assertEqual(self.fk['fk_rental_customer'].drop(), "DROP FOREIGN KEY `fk_rental_customer`")

    def test_FK_create(self):
        self.assertEqual(self.fk['fk_rental_customer'].create(),
                        "ADD CONSTRAINT `fk_rental_customer` FOREIGN KEY `fk_rental_customer` (`customer_id`) REFERENCES `customer` (`customer_id`) ON DELETE RESTRICT ON UPDATE CASCADE")

    def test_FK_eq(self):
        self.assertEqual(self.fk['fk_rental_customer'], self.fk['fk_rental_customer'])

    def test_FK_neq(self):
        self.assertNotEqual(self.fk['fk_rental_customer'], self.fk['fk_rental_inventory'])

    def test_FK_reference_opts_update_and_delete(self):
        table_def = """CREATE TABLE `child` (
                      `id` int(11) DEFAULT NULL,
                      `parent_id` int(11) DEFAULT NULL,
                      KEY `par_ind` (`parent_id`),
                      CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE SET NULL ON UPDATE CASCADE.
                      CONSTRAINT `child_ibfk_2` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
                      ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_danish_ci COMMENT='hello world';"""

        matches = re.search(REGEX_FK_REFERENCE_OPTIONS % 'child_ibfk_1', table_def,  re.X)
        self.assertTrue(matches)
        self.assertTrue(matches.group('on_delete'))
        self.assertTrue(matches.group('on_update'))
        self.assertEqual(matches.group('on_delete'), 'SET NULL')
        self.assertEqual(matches.group('on_update'), 'CASCADE')

        matches = re.search(REGEX_FK_REFERENCE_OPTIONS % 'child_ibfk_2', table_def,  re.X)
        self.assertTrue(matches)
        self.assertTrue(matches.group('on_delete'))
        self.assertTrue(matches.group('on_update'))
        self.assertEqual(matches.group('on_delete'), 'RESTRICT')
        self.assertEqual(matches.group('on_update'), 'RESTRICT')

    def test_FK_reference_opts_delete(self):
        table_def = """CREATE TABLE `child` (
                      `id` int(11) DEFAULT NULL,
                      `parent_id` int(11) DEFAULT NULL,
                      KEY `par_ind` (`parent_id`),
                      CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE SET NULL
                      ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_danish_ci COMMENT='hello world';"""

        matches = re.search(REGEX_FK_REFERENCE_OPTIONS % 'child_ibfk_1', table_def,  re.X)
        self.assertTrue(matches)
        self.assertTrue(matches.group('on_delete'))
        self.assertTrue(not matches.group('on_update'))
        self.assertEqual(matches.group('on_delete'), 'SET NULL')

    def test_FK_reference_opts_update(self):
        table_def = """CREATE TABLE `child` (
                      `id` int(11) DEFAULT NULL,
                      `parent_id` int(11) DEFAULT NULL,
                      KEY `par_ind` (`parent_id`),
                      CONSTRAINT `child_ibfk_1` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON UPDATE CASCADE
                      ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_danish_ci COMMENT='hello world';"""

        matches = re.search(REGEX_FK_REFERENCE_OPTIONS % 'child_ibfk_1', table_def,  re.X)
        self.assertTrue(matches)
        self.assertTrue(not matches.group('on_delete'))
        self.assertTrue(matches.group('on_update'))
        self.assertEqual(matches.group('on_update'), 'CASCADE')


if __name__ == "__main__":
    from test_all import get_database_url
    TestForeignKeySchema.database_url = get_database_url()
    unittest.main()