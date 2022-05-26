# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import re
import sys
import unittest
"""
TODO docs

Includes a main routine that can run the unit tests, or invoke table generator
with command line arguments to generate a single table
"""


class DDLGenerator(object):
    def __init__(self):
        pass

    DEFAULT_COLUMNS = [
        {'cname': 'txnid',                     'ctype': 'BIGINT',        'cattribs': 'NOT NULL'},
        {'cname': 'rowid',                     'ctype': 'BIGINT',        'cattribs': 'NOT NULL'},
        {'cname': 'rowid_group',               'ctype': 'TINYINT',       'cattribs': 'NOT NULL'},
        {'cname': 'type_null_tinyint',         'ctype': 'TINYINT',       'cattribs': ''},
        {'cname': 'type_not_null_tinyint',     'ctype': 'TINYINT',       'cattribs': ' NOT NULL'},
        {'cname': 'type_null_smallint',        'ctype': 'SMALLINT',      'cattribs': ''},
        {'cname': 'type_not_null_smallint',    'ctype': 'SMALLINT',      'cattribs': 'NOT NULL'},
        {'cname': 'type_null_integer',         'ctype': 'INTEGER',       'cattribs': ''},
        {'cname': 'type_not_null_integer',     'ctype': 'INTEGER',       'cattribs': 'NOT NULL'},
        {'cname': 'type_null_bigint',          'ctype': 'BIGINT',        'cattribs': ''},
        {'cname': 'type_not_null_bigint',      'ctype': 'BIGINT',        'cattribs': 'NOT NULL'},
        {'cname': 'type_null_timestamp',       'ctype': 'TIMESTAMP',     'cattribs': ''},
        {'cname': 'type_not_null_timestamp',   'ctype': 'TIMESTAMP',     'cattribs': 'DEFAULT NOW NOT NULL'},
        {'cname': 'type_null_decimal',         'ctype': 'DECIMAL',       'cattribs': ''},
        {'cname': 'type_not_null_decimal',     'ctype': 'DECIMAL',       'cattribs': ' NOT NULL'},
        {'cname': 'type_null_float',           'ctype': 'FLOAT',         'cattribs': ''},
        {'cname': 'type_not_null_float',       'ctype': 'FLOAT',         'cattribs': 'NOT NULL'},
        {'cname': 'type_null_varchar25',       'ctype': 'VARCHAR(32)',   'cattribs': ''},
        {'cname': 'type_not_null_varchar25',   'ctype': 'VARCHAR(32)',   'cattribs': 'NOT NULL'},
        {'cname': 'type_null_varchar128',      'ctype': 'VARCHAR(128)',  'cattribs': ''},
        {'cname': 'type_not_null_varchar128',  'ctype': 'VARCHAR(128)',  'cattribs': 'NOT NULL'},
        {'cname': 'type_null_varchar1024',     'ctype': 'VARCHAR(1024)', 'cattribs': ''},
        {'cname': 'type_not_null_varchar1024', 'ctype': 'VARCHAR(1024)', 'cattribs': 'NOT NULL'},
    ]

    METADATA_COLUMNS = [
        {'cname': 'VOLT_TRANSACTION_ID',         'ctype': 'BIGINT',  'cattribs': ''},
        {'cname': 'VOLT_EXPORT_TIMESTAMP',       'ctype': 'BIGINT',  'cattribs': ''},
        {'cname': 'VOLT_EXPORT_SEQUENCE_NUMBER', 'ctype': 'BIGINT',  'cattribs': ''},
        {'cname': 'VOLT_PARTITION_ID',           'ctype': 'BIGINT',  'cattribs': ''},
        {'cname': 'VOLT_SITE_ID',                'ctype': 'BIGINT',  'cattribs': ''},
        {'cname': 'VOLT_EXPORT_OPERATION',       'ctype': 'TINYINT', 'cattribs': ''},
    ]

    GEODATA_COLUMNS = [
        {'cname': 'type_null_geography',           'ctype': 'GEOGRAPHY(1024)',  'cattribs': ''},
        {'cname': 'type_not_null_geography',       'ctype': 'GEOGRAPHY(1024)',  'cattribs': 'NOT NULL'},
        {'cname': 'type_null_geography_point',     'ctype': 'GEOGRAPHY_POINT',  'cattribs': ''},
        {'cname': 'type_not_null_geography_point', 'ctype': 'GEOGRAPHY_POINT',  'cattribs': 'NOT NULL'},
    ]

    @staticmethod
    def _gen_column_details(columns, first_comma=False):
        """
        Generate column definition DDL for the given list of columns
        :param columns: list of dict. dictionary elements should have
            cname, ctype, and (optional) cattribs
        :param first_comma: Whether or not to include a comma before the
            first field generated
        :return: a partial SQL expression
        """
        data = ''
        comma = first_comma

        for col in columns:
            if comma:
                data += ', '
            else:
                data += '  '
                comma = True
            data += col['cname'] + ' ' + col['ctype']
            if col['cattribs']:
                data += " " + col['cattribs']
            data += '\n'
        return data

    def _gen_common_cols(self, metadata, geocolumns):
        """
        Handle the details of generating DDL for the common columns
        all tables include and optionally generating DDL for the metadata
        and geolocation columns some tables include.  column definitions are
        shared by streams and tables.
        """
        sql = ""
        if metadata:
            sql += self._gen_column_details(self.METADATA_COLUMNS)
            sql += self._gen_column_details(self.DEFAULT_COLUMNS, first_comma=True)
        else:
            sql += self._gen_column_details(self.DEFAULT_COLUMNS)

        if geocolumns:
            sql += self._gen_column_details(self.GEODATA_COLUMNS, first_comma=True)

        return sql

    # TODO index=field. Or should INDEX be a separate token like %%INDEX?
    # TODO search METADATA_COLUMNS if primary_key column not in default columns and metadata enabled?
    # TODO maybe validate export_cdc is one or more comma separated INSERT, DELETE, UPDATE_OLD, UPDATE_NEW, UPDATE?
    def gen_table(self, name,
                  primary_key=None, partition_key=None,
                  metadata=False,
                  geocolumns=False,
                  export_target=None, export_cdc=None,
                  migrate_target=None,
                  migrate_topic=None,
                  export_topic=None,
                  ttl_index=None, ttl_value=None,
                  ttl_batch_size=None,
                  ttl_max_frequency=None
    ):
        if export_target and migrate_target:
            raise AttributeError("export_target and migrate_target are mutually exclusive")
        if ttl_index is not None and ttl_value is None:
            raise AttributeError("when ttl_index is specified, ttl_value must be also be set")
        if ttl_index is None and ttl_value is not None:
            raise AttributeError("when ttl_value is specified, ttl_index must be also be set")

        sql = "CREATE TABLE " + name

        if export_target:
            sql += " EXPORT TO TARGET " + export_target
        elif migrate_target:
            sql += " MIGRATE TO TARGET " + migrate_target
        elif migrate_topic:
            sql += " MIGRATE TO TOPIC " + migrate_topic
        elif export_topic:
            sql += " EXPORT TO TOPIC " + export_topic

        if export_cdc:
            if not export_target and not export_topic:
                raise ValueError("export_target or export_topic argument required when export_cdc specified")
            sql += " ON " + export_cdc

        sql += "\n(\n"

        sql += self._gen_common_cols(metadata, geocolumns)

        if primary_key:
            if not any(d['cname'] == primary_key for d in self.DEFAULT_COLUMNS):
                raise AttributeError("primary key column '" + primary_key + "' not present in ALL_COLUMNS")
            sql += ", PRIMARY KEY (" + primary_key + ")\n"

        sql += ")"

        if ttl_index:
            # USING TTL value [time-unit] ON COLUMN column-name
            # [BATCH_SIZE number-of-rows] [MAX_FREQUENCY value]
            sql += " USING TTL " + str(ttl_value) + " ON COLUMN " + ttl_index
            if ttl_batch_size:
                sql += " BATCH_SIZE " + str(ttl_batch_size)
            if ttl_max_frequency:
                sql += " MAX_FREQUENCY " + str(ttl_max_frequency)
        sql += ";"

        if partition_key:
            if not any(d['cname'] == partition_key for d in self.DEFAULT_COLUMNS):
                raise AttributeError("partition column '" + partition_key + "' not present in ALL_COLUMNS")

            sql += "\nPARTITION TABLE " + name + " ON COLUMN " + partition_key + ";"

        return sql

    def gen_stream(self, name,
                   partition_key=None,
                   metadata=False,
                   geocolumns=False,
                   topic_target=False,
                   export_target=None,
                   topic_key=False):
        sql = "CREATE STREAM " + name

        if partition_key:
            if not any(d['cname'] == partition_key for d in self.DEFAULT_COLUMNS):
                raise AttributeError("partition column '" + partition_key + "' not present in ALL_COLUMNS")
            sql += " PARTITION ON COLUMN " + partition_key

        if export_target:
            sql += " EXPORT TO TARGET " + export_target
        elif topic_target:
            sql += " EXPORT TO TOPIC " + topic_target
            if topic_key:
                sql += " WITH KEY (" + topic_key + ") "

        sql += "\n(\n"

        sql += self._gen_common_cols(metadata, geocolumns)

        sql += ");"

        return sql

# =============================================================================


class TestDDLGenerator(unittest.TestCase):
    @staticmethod
    def normalize_whitespace(data):
        """
        replace repeat whitespace characters with a single whitespace character
        """
        return re.sub(r"\s\s+", " ", data)

    def test_table_basic_no_partitioning(self):
        self.maxDiff = None
        tg = DDLGenerator()
        sql = tg.gen_table('basic_table')

        expected = """CREATE TABLE basic_table
(
  txnid BIGINT NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);"""
        nwe = self.normalize_whitespace(expected)
        nws = self.normalize_whitespace(sql)
        self.assertMultiLineEqual(nws, nwe)

    def test_table_with_primary_key(self):
        tg = DDLGenerator()
        sql = tg.gen_table('basic_table', primary_key='rowid')
        self.assertRegexpMatches(sql, ".*, PRIMARY KEY \\(rowid\\)\n\\);")
        pass

    def test_table_neg_invalid_primary_key(self):
        tg = DDLGenerator()
        with self.assertRaises(AttributeError) as cm:
            tg.gen_table('invalid_table', primary_key='noexist')
        self.assertEqual(str(cm.exception), "primary key column 'noexist' not present in ALL_COLUMNS")

    def test_table_partition_key(self):
        tg = DDLGenerator()
        sql = tg.gen_table('moo', partition_key='rowid')
        self.assertRegexpMatches(sql, ".*\nPARTITION TABLE moo ON COLUMN rowid;")

    def test_table_neg_invalid_partition_key(self):
        tg = DDLGenerator()
        with self.assertRaises(AttributeError) as cm:
            tg.gen_table('invalid_table', partition_key='noexist')
        self.assertEqual(str(cm.exception), "partition column 'noexist' not present in ALL_COLUMNS")

    def test_table_metadata_included(self):
        tg = DDLGenerator()
        sql = tg.gen_table("elephant", metadata=True)
        nwe = self.normalize_whitespace("""(?s)CREATE TABLE elephant
\\(
  VOLT_TRANSACTION_ID       BIGINT
, VOLT_EXPORT_TIMESTAMP     BIGINT
, VOLT_EXPORT_SEQUENCE_NUMBER BIGINT
, VOLT_PARTITION_ID         BIGINT
, VOLT_SITE_ID              BIGINT
, VOLT_EXPORT_OPERATION     TINYINT
, txnid BIGINT NOT NULL
, rowid                     BIGINT        NOT NULL
.*
\\);""")
        nws = self.normalize_whitespace(sql)
        self.assertRegexpMatches(nws, nwe)

    def test_table_geocolumns_included(self):
        tg = DDLGenerator()
        sql = tg.gen_table("desert", geocolumns=True)
        nws = self.normalize_whitespace(sql)
        nwe = self.normalize_whitespace("""(?s)CREATE TABLE desert
\\(
  txnid BIGINT NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
.*
, type_null_geography           GEOGRAPHY\\(1024\\)
, type_not_null_geography       GEOGRAPHY\\(1024\\)  NOT NULL
, type_null_geography_point     GEOGRAPHY_POINT
, type_not_null_geography_point GEOGRAPHY_POINT      NOT NULL
\\);""")

        self.assertRegexpMatches(nws, nwe)

    def test_table_export_target(self):
        tg = DDLGenerator()
        sql = tg.gen_table("albatross", export_target='loopback_target')
        self.assertRegexpMatches(sql, '(?s)CREATE TABLE albatross EXPORT TO TARGET loopback_target.*')

    def test_table_export_target_cdc1(self):
        tg = DDLGenerator()
        sql = tg.gen_table("meltdown", export_target='kafka_target', export_cdc='DELETE, UPDATE_NEW')
        self.assertRegexpMatches(sql, '(?s)CREATE TABLE meltdown EXPORT TO TARGET kafka_target ON DELETE, UPDATE_NEW.*')

    def test_table_neg_export_cdc_set_target_not(self):
        tg = DDLGenerator()
        with self.assertRaises(ValueError) as cm:
            tg.gen_table('nope', export_cdc='UPDATE')
        self.assertEqual(str(cm.exception), "export_target argument required when export_cdc specified")

    def test_table_partkey_and_export_target(self):
        tg = DDLGenerator()
        sql = tg.gen_table("trestle",
                           partition_key='rowid',
                           migrate_target='pedestal')
        self.assertRegexpMatches(sql, "(?s)CREATE TABLE trestle MIGRATE TO TARGET pedestal.*;\n"
                                      "PARTITION TABLE trestle ON COLUMN rowid;")

    def test_table_migrate_target(self):
        tg = DDLGenerator()
        sql = tg.gen_table("albatross", export_target='target29')
        self.assertRegexpMatches(sql, '(?s)CREATE TABLE albatross EXPORT TO TARGET target29.*')

    def test_table_neg_migrate_and_export_targets(self):
        tg = DDLGenerator()
        with self.assertRaises(AttributeError) as cm:
            tg.gen_table("tea", export_target='green', migrate_target='oolong')
        self.assertEqual(str(cm.exception), "export_target and migrate_target are mutually exclusive")

    def test_table_ttl_basic(self):
        tg = DDLGenerator()
        sql = tg.gen_table("pembroke", ttl_index='type_not_null_timestamp', ttl_value="5" )
        self.assertRegexpMatches(sql, '(?s)CREATE TABLE pembroke.*\) USING TTL 5 ON COLUMN type_not_null_timestamp;')

    def test_table_ttl_minutes(self):
        tg = DDLGenerator()
        sql = tg.gen_table("kang", ttl_index='type_not_null_timestamp', ttl_value="15 MINUTES" )
        self.assertRegexpMatches(sql, '(?s)CREATE TABLE kang.*\) USING TTL 15 MINUTES ON COLUMN type_not_null_timestamp;')

    def test_table_ttl_batch_size(self):
        tg = DDLGenerator()
        sql = tg.gen_table("kang", ttl_index='type_not_null_timestamp', ttl_value="3", ttl_batch_size=27)
        self.assertRegexpMatches(sql, '(?s)CREATE TABLE kang.*\\) USING TTL 3 ON COLUMN type_not_null_timestamp BATCH_SIZE 27;')

    def test_table_max_ttl_frequency(self):
        tg = DDLGenerator()
        sql = tg.gen_table("hutch", ttl_index='type_not_null_timestamp', ttl_value="11", ttl_max_frequency=14)
        self.assertRegexpMatches(sql, '(?s)CREATE TABLE hutch.*\\) USING TTL 11 ON COLUMN type_not_null_timestamp MAX_FREQUENCY 14;')

    def test_table_neg_ttl_index_no_value(self):
        tg = DDLGenerator()
        with self.assertRaises(AttributeError) as cm:
            tg.gen_table("butch", ttl_index="meltdown")
        self.assertEqual(str(cm.exception), "when ttl_index is specified, ttl_value must be also be set")

    def test_table_neg_ttl_value_no_index(self):
        tg = DDLGenerator()
        with self.assertRaises(AttributeError) as cm:
            tg.gen_table("sundance", ttl_value="freezeup")
        self.assertEqual(str(cm.exception), "when ttl_value is specified, ttl_index must be also be set")


    # ========================================================================

    def test_stream_basic_no_partitioning(self):
        self.maxDiff = None
        tg = DDLGenerator()
        sql = tg.gen_stream('stream1')

        expected = """CREATE STREAM stream1
(
  txnid BIGINT NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
, type_null_tinyint         TINYINT
, type_not_null_tinyint     TINYINT       NOT NULL
, type_null_smallint        SMALLINT
, type_not_null_smallint    SMALLINT      NOT NULL
, type_null_integer         INTEGER
, type_not_null_integer     INTEGER       NOT NULL
, type_null_bigint          BIGINT
, type_not_null_bigint      BIGINT        NOT NULL
, type_null_timestamp       TIMESTAMP
, type_not_null_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL
, type_null_decimal         DECIMAL
, type_not_null_decimal     DECIMAL       NOT NULL
, type_null_float           FLOAT
, type_not_null_float       FLOAT         NOT NULL
, type_null_varchar25       VARCHAR(32)
, type_not_null_varchar25   VARCHAR(32)   NOT NULL
, type_null_varchar128      VARCHAR(128)
, type_not_null_varchar128  VARCHAR(128)  NOT NULL
, type_null_varchar1024     VARCHAR(1024)
, type_not_null_varchar1024 VARCHAR(1024) NOT NULL
);"""
        nwe = self.normalize_whitespace(expected)
        nws = self.normalize_whitespace(sql)
        self.assertMultiLineEqual(nws, nwe)

    def test_stream_metadata_included(self):
        tg = DDLGenerator()
        sql = tg.gen_stream("chicopee", metadata=True)
        nwe = self.normalize_whitespace("""(?s)CREATE STREAM chicopee
\\(
  VOLT_TRANSACTION_ID       BIGINT
, VOLT_EXPORT_TIMESTAMP     BIGINT
, VOLT_EXPORT_SEQUENCE_NUMBER BIGINT
, VOLT_PARTITION_ID         BIGINT
, VOLT_SITE_ID              BIGINT
, VOLT_EXPORT_OPERATION     TINYINT
, txnid BIGINT NOT NULL
, rowid                     BIGINT        NOT NULL
.*
\\);""")
        nws = self.normalize_whitespace(sql)
        self.assertRegexpMatches(nws, nwe)

    def test_stream_geocolumns_included(self):
        tg = DDLGenerator()
        sql = tg.gen_stream("charles", geocolumns=True)
        nws = self.normalize_whitespace(sql)
        nwe = self.normalize_whitespace("""(?s)CREATE STREAM charles
\\(
  txnid BIGINT NOT NULL
, rowid                     BIGINT        NOT NULL
, rowid_group               TINYINT       NOT NULL
.*
, type_null_geography           GEOGRAPHY\\(1024\\)
, type_not_null_geography       GEOGRAPHY\\(1024\\)  NOT NULL
, type_null_geography_point     GEOGRAPHY_POINT
, type_not_null_geography_point GEOGRAPHY_POINT      NOT NULL
\\);""")

        self.assertRegexpMatches(nws, nwe)

    def test_stream_partition_key(self):
        tg = DDLGenerator()
        sql = tg.gen_stream('moo', partition_key='rowid')
        self.assertRegexpMatches(sql, "CREATE STREAM moo PARTITION ON COLUMN rowid.*")

    def test_stream_export_target(self):
        tg = DDLGenerator()
        sql = tg.gen_stream("concord", export_target='kafka_t572')
        self.assertRegexpMatches(sql, '(?s)CREATE STREAM concord EXPORT TO TARGET kafka_t572.*')

    def test_stream_partkey_and_export_target(self):
        tg = DDLGenerator()
        sql = tg.gen_stream("doublemint",
                            partition_key='rowid',
                            export_target='bullseye')
        self.assertRegexpMatches(sql, "CREATE STREAM doublemint PARTITION ON COLUMN rowid EXPORT TO TARGET bullseye.*")

# ============================================================================


if __name__ == '__main__':
    """
    A simple wrapper around either running the unit tests contained here-in or running
    the table generator method with parameters passed along from the command line
    """
    if "--unit" in sys.argv:
        sys.argv.remove("--unit")
        ut = unittest.main()
        sys.exit(not ut.wasSuccessful())
    else:
        generator = DDLGenerator()
        params = {}
        for arg in sys.argv[1:]:
            k, v = arg.split("=", 1)
            params[k] = v
        print(generator.gen_table(**params))
