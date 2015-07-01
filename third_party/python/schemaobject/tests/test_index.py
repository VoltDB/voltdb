#!/usr/bin/python
import unittest
import schemaobject

class TestIndexSchema(unittest.TestCase):
    def setUp(self):
            self.db = schemaobject.SchemaObject(self.database_url + 'sakila')
            self.db = self.db.selected

    def test_format_sub_part_with_length(self):
        self.assertEqual('`name`(16)', schemaobject.index.IndexSchema.format_sub_part('name', 16))

    def test_format_sub_part_without_length(self):
        self.assertEqual('`name`', schemaobject.index.IndexSchema.format_sub_part('name', 0))
        self.assertEqual('`name`', schemaobject.index.IndexSchema.format_sub_part('name', None))

    def test_index_exists(self):
        assert "idx_fk_address_id" in self.db.tables['customer'].indexes

    def test_index_name(self):
        self.assertEqual("idx_fk_address_id", self.db.tables['customer'].indexes['idx_fk_address_id'].name)

    def test_index_table_name(self):
        self.assertEqual("customer", self.db.tables['customer'].indexes['idx_fk_address_id'].table_name)

    def test_index_kind(self):
        self.assertEqual("BTREE", self.db.tables['customer'].indexes['idx_fk_address_id'].type)

    def test_index_type_primary_key(self):
        self.assertEqual("PRIMARY", self.db.tables['customer'].indexes['PRIMARY'].kind)

    def test_index_(self):
        self.assertEqual("INDEX", self.db.tables['rental'].indexes['idx_fk_customer_id'].kind)

    def test_index_non_unique(self):
        self.assertEqual(True, self.db.tables['rental'].indexes['idx_fk_inventory_id'].non_unique)

    def test_index_unique(self):
        self.assertEqual(False, self.db.tables['rental'].indexes['rental_date'].non_unique)
        self.assertEqual("UNIQUE", self.db.tables['rental'].indexes['rental_date'].kind)

    def test_index_comment(self):
        self.assertEqual("", self.db.tables['customer'].indexes['idx_fk_address_id'].comment)

    def test_index_collation(self):
        self.assertEqual("A", self.db.tables['customer'].indexes['idx_fk_address_id'].collation)

    def test_index_fields_correct_order(self):
        self.assertEqual(self.db.tables['rental'].indexes['rental_date'].fields,
                    [('rental_date', 0), ('inventory_id', 0), ('customer_id', 0)])

    def test_index_fields_incorrect_order(self):
        self.assertNotEqual(self.db.tables['rental'].indexes['rental_date'].fields,
                    [('inventory_id', 0), ('rental_date', 0), ('customer_id', 0)] )

    def test_index_eq(self):
        self.assertEqual(self.db.tables['rental'].indexes['rental_date'],
                        self.db.tables['rental'].indexes['rental_date'])

    def test_index_neq(self):
        self.assertNotEqual(self.db.tables['rental'].indexes['rental_date'],
                        self.db.tables['customer'].indexes['idx_fk_address_id'])

    def test_drop_index(self):
        self.assertEqual(self.db.tables['rental'].indexes['rental_date'].drop(),
                        "DROP INDEX `rental_date`")

    def test_drop_primary_key(self):
        self.assertEqual(self.db.tables['customer'].indexes['PRIMARY'].drop(),
                        "DROP PRIMARY KEY")

    def test_add_primary_key(self):
        self.assertEqual(self.db.tables['customer'].indexes['PRIMARY'].create(),
                        "ADD PRIMARY KEY (`customer_id`) USING BTREE")

    def test_add_unique_index(self):
        self.assertEqual(self.db.tables['rental'].indexes['rental_date'].create(),
                        "ADD UNIQUE INDEX `rental_date` (`rental_date`, `inventory_id`, `customer_id`) USING BTREE")

    def test_add_fulltext_index(self):
        self.assertEqual(self.db.tables['film_text'].indexes['idx_title_description'].create(),
                        "ADD FULLTEXT INDEX `idx_title_description` (`title`, `description`)")

    def test_add_index_using_BTREE(self):
      self.assertEqual(self.db.tables['payment'].indexes['idx_fk_staff_id'].create(),
                      "ADD INDEX `idx_fk_staff_id` (`staff_id`) USING BTREE")

    def test_add_index_using_HASH(self):
        assert False, "Add index using HASH to test DB"

    def test_add_index_using_RTREE(self):
        assert False, "Add index using RTREE to test DB"

    def test_add_spatial_index(self):
        assert False, "Add spatial index to test DB"

    def test_add_index_with_subpart(self):
        assert False, "Add subparts to indicies in test DB"


if __name__ == "__main__":
    from test_all import get_database_url
    TestIndexSchema.database_url = get_database_url()
    unittest.main()