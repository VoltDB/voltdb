#!/usr/bin/python
import unittest
from test_schema import TestSchema
from test_database import TestDatabaseSchema
from test_table import TestTableSchema
from test_column import TestColumnSchema
from test_index import TestIndexSchema
from test_foreignkey import TestForeignKeySchema
from test_option import TestOptionSchema
from test_collections import TestOrderedDict
from test_connection import TestDatabaseURL

def get_database_url():
    database_url = raw_input("\nTests need to be run against the Sakila Database v0.8\n"
                            "Enter the MySQL Database Connection URL without the database name\n"
                            "Example: mysql://user:pass@host:port/\n"
                            "URL: ")
    if not database_url.endswith('/'):
        database_url += '/'
    return database_url

def regressionTest():
    test_cases = [
                  TestSchema,
                  TestDatabaseSchema,
                  TestTableSchema,
                  TestColumnSchema,
                  TestIndexSchema,
                  TestForeignKeySchema,
                  TestOptionSchema,
                  TestOrderedDict,

                  ]
    database_url = get_database_url()

    suite = unittest.TestSuite()
    for tc in test_cases:
        tc.database_url = database_url
        suite.addTest(unittest.makeSuite(tc))
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest="regressionTest")