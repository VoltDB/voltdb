# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
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

import sys
import os
import re
from voltcli import utility
from voltcli import environment

# A common OS X mysql installation problem can prevent _mysql.so from loading.
# Postpone database imports until initialize() is called to avoid errors in
# non-mysql commands and to provide good messages to help diagnose errors.
MySQLdb = None

# Find and import the voltdb-patched schemaobject module.
# It should be in third_party/python/schemaobject.
# Prepend it to the Python path so that it overrides any local unpatched copy.
if environment.third_party_python is None:
    utility.abort('Third party python libraries are not available')
schemaobject_path = os.path.join(environment.third_party_python, 'schemaobject')
sys.path.insert(0, schemaobject_path)
import schemaobject

# IMPORTANT: Any name being generated needs to be post-processed with
# fix_name() at the point where the DDL is being output, but no sooner. All
# schema lookups are with un-fixed symbols, but all names that conflict with
# keywords need to be tweaked upon output.

class Global:
    """
    Globals.
    """
    # Regular expression to parse a MySQL type.
    re_type = re.compile(
            # Whitespace
            r'^\s'
            # Token (group 1)
            r'*\b([a-z0-9_ ]+)\b'
            # Whitespace
            r'\s*'
            # Optional [MODIFIER] (group 2)
            r'([(]([^)]*)[)])?'
            # Whitespace
            r'\s*$')
    # The suggestion to display when OS X mysql module import fails.
    osx_fix = 'ln -s /usr/local/mysql/lib/libmysqlclient.[0-9][0-9]*.dylib /usr/local/lib/'
    # The comment prefix used to annotate generated lines.
    vcomment_prefix = 'GEN:'


# Conversion error/warning messages.
MESSAGES = utility.MessageDict(
    WIDTH_PARAM = 'Width parameter is not supported.',
    MEDIUM_INT = '3 byte integer was expanded to 4 bytes.',
    PREC_PARAMS = 'Precision and scale parameters are not supported.',
    BIT_TYPE = 'BIT type was replaced by VARBINARY.',
    DATE_TIME = 'Date/time type was replaced by TIMESTAMP.',
    ENUM = 'ENUM type was replaced by VARCHAR.',
    SET = 'SET type was replaced by VARCHAR.',
    CHAR = 'CHAR type was replaced by VARCHAR.',
    TEXT = 'TEXT type was replaced by VARCHAR.',
    BINARY = 'BINARY type was replaced by VARBINARY.',
    BLOB = 'BLOB type was replaced by VARBINARY.',
    UNSUPPORTED = 'Unsupported MySQL type.',
    PARSE_ERROR = 'Unable to parse.',
)

# From HSQLDB Tokens.java.
# Keep this list all upper-case or change fix_name() accordingly.
KEYWORDS = [
    'ADMIN', 'AS', 'AND', 'ALL', 'ANY', 'AT', 'AVG',
    'BY', 'BETWEEN', 'BOTH',
    'CALL', 'CASE', 'CAST', 'CORRESPONDING', 'CONVERT', 'COUNT', 'COALESCE', 'CREATE', 'CROSS',
    'DISTINCT', 'DROP',
    'ELSE', 'END', 'EVERY', 'EXISTS', 'EXCEPT',
    'FOR', 'FROM', 'FULL',
    'GRANT', 'GROUP',
    'HAVING',
    'INTO', 'IS', 'IN', 'INTERSECT', 'JOIN', 'INNER',
    'LEFT', 'LEADING', 'LIKE',
    'MAX', 'MIN',
    'NATURAL', 'NULLIF', 'NOT',
    'ON', 'ORDER', 'OR', 'OUTER',
    'PRIMARY',
    'REFERENCES', 'RIGHT',
    'SELECT', 'SET', 'SOME', 'STDDEV_POP', 'STDDEV_SAMP', 'SUM',
    'TABLE', 'THEN', 'TO', 'TRAILING', 'TRIGGER',
    'UNION', 'UNIQUE', 'USING',
    'VALUES', 'VAR_POP', 'VAR_SAMP',
    'WHEN', 'WHERE', 'WITH'
]

def initialize():
    """
    Perform various one-time initializations.
    """
    if MySQLdb is None:
        initialize_mysql_imports()


def initialize_mysql_imports():
    """
    Perform the delayed MySQL module imports.
    """
    try:
        import MySQLdb as MySQLdb_
        global MySQLdb
        MySQLdb = MySQLdb_
    except ImportError, e:
        msgs = [
            'Failed to import MySQL module.',
            'Is MySQL properly installed?',
                ('http://dev.mysql.com/downloads/',)
        ]
        if sys.platform == 'darwin':
            msgs.extend([
                'On OS X you may need to create a symlink, e.g.',
                    (Global.osx_fix,)
            ])
        msgs.append(e)
        utility.abort(*msgs)


def generate_schema(uri, partition_table, output_stream):
    initialize()
    schema_generator = MySQLSchemaGenerator(uri, partition_table)
    schema_generator.write_schema(output_stream)
    return schema_generator


def fix_name(name):
    if name.upper() not in KEYWORDS:
        return name
    return '%s_' % name

class MySQLTable(object):
    def __init__(self, schema, row_count):
        self.schema = schema
        self.row_count = row_count
        self.name = schema.name
        self.primary_key_columns = []
        for column_name in self.schema.columns:
            column = self.schema.columns[column_name]
            if column.key == 'PRI':
                self.primary_key_columns.append(column)


class PartitionedTable(object):
    def __init__(self, table, pkey_column_name, reason_chosen):
        self.table = table
        self.name = table.name
        self.pkey_column_name = pkey_column_name
        self.reason_chosen = reason_chosen


class MySQLSchemaGenerator(object):

    def __init__(self, uri, partition_table):
        self.uri = uri
        self.partition_table_name = partition_table
        self.formatter = utility.CodeFormatter(vcomment_prefix=Global.vcomment_prefix)
        # Initialized in initialize()
        self.schema = None
        self.database = None
        # Maps table names to MySQLTable objects.
        self.tables = {}
        self.table_names = []
        # Maps partitioned table names to PartitionedTable objects.
        self.partitioned_tables = {}

    #=== Public methods.

    def write_schema(self, f):
        # Handle broken pipes, e.g. when stdout is piped to more and the user presses 'q'.
        try:
            f.write(self.format_schema())
        except IOError, e:
            if f != sys.stdout:
                utility.abort(e)
            else:
                utility.abort()

    def format_schema(self):
        self._initialize()
        self.formatter.vcomment('::: Generated by %s :::' % environment.command_name,
                                'Generated comments are prefixed by "%s"' % Global.vcomment_prefix)
        for table_name in self.table_names:
            self.format_table(table_name)
            self.format_table_indexes(table_name)
            self.format_table_partitioning(table_name)
        return str(self.formatter)

    def format_table(self, table_name):
        table = self.get_table(table_name)
        table_comment = table.schema.options.get('comment', '').value
        if table_comment:
            self.formatter.comment(table_comment)
        self.formatter.block_start('CREATE TABLE %s' % fix_name(table_name))
        for column_name in table.schema.columns:
            self.format_column(table_name, column_name)
        self.format_primary_key(table_name)
        self.formatter.block_end()
        self.format_foreign_keys(table_name)

    def format_foreign_keys(self, table_name):
        table = self.get_table(table_name)
        if table.schema.foreign_keys:
            self.formatter.blank()
            for fk_name in sorted(table.schema.foreign_keys.keys()):
                fk = table.schema.foreign_keys[fk_name]
                self.formatter.comment('Foreign key: %s' % fix_name(fk.name)),
                for i in range(len(fk.columns)):
                    self.formatter.comment('  %s.%s -> %s.%s'
                                                % (fix_name(table.name),
                                                   fix_name(fk.columns[i]),
                                                   fix_name(fk.referenced_table_name),
                                                   fix_name(fk.referenced_columns[i])))

    def format_table_partitioning(self, table_name):
        if table_name in self.partitioned_tables:
            ptable = self.partitioned_tables[table_name]
            self.formatter.blank()
            self.formatter.vcomment(ptable.reason_chosen)
            self.formatter.code('PARTITION TABLE %s ON COLUMN %s;'
                                    % (fix_name(ptable.name),
                                       fix_name(ptable.pkey_column_name)))

    def format_table_indexes(self, table_name):
        table = self.get_table(table_name)
        nindex = 0
        for index_name in sorted(table.schema.indexes.keys()):
            index = table.schema.indexes[index_name]
            if index.kind != 'PRIMARY':
                nindex += 1
                if index.comment:
                    self.formatter.comment(index.comment)
                if index.non_unique:
                    unique = ''
                else:
                    unique = ' UNIQUE'
                self.formatter.block_start('CREATE%s INDEX IDX_%s_%s ON %s'
                                                % (unique, fix_name(table_name),
                                                           fix_name(index.name),
                                                           fix_name(table_name)))
                for column_name, sub_part in index.fields:
                    self.formatter.code_fragment(fix_name(column_name))
                    if sub_part != 0:
                        self.formatter.block_vcomment('Ignored sub-partition %d on field "%s".'
                                                            % (sub_part, fix_name(column_name)))
                self.formatter.block_end()

    def format_column(self, table_name, column_name):
        table = self.get_table(table_name)
        column = table.schema.columns[column_name]
        if column.comment:
            self.formatter.comment(column.comment)
        voltdb_type, message_number = convert_type(column.type)
        if message_number:
            self.formatter.block_vcomment('column #%d %s %s: %s'
                                                % (column.ordinal_position,
                                                   fix_name(column.name),
                                                   column.type,
                                                   MESSAGES[message_number]))
        if column.null:
            voltdb_null = ''
        else:
            voltdb_null = ' NOT NULL'
        self.formatter.code_fragment('%s %s%s' % (fix_name(column.name), voltdb_type, voltdb_null))

    def format_primary_key(self, table_name):
        table = self.get_table(table_name)
        if table.primary_key_columns:
            self.formatter.blank()
            self.formatter.block_start('CONSTRAINT PK_%s PRIMARY KEY' % fix_name(table_name))
            for primary_key_column in table.primary_key_columns:
                self.formatter.code_fragment(fix_name(primary_key_column.name))
            self.formatter.block_end()

    def get_table(self, table_name):
        try:
            return self.tables[table_name]
        except KeyError:
            utility.abort('Attempted to access schema for unknown table "%s".' % table_name)

    def execute(self, query, *args):
        try:
            return self.schema.connection.execute(query, args)
        except Exception, e:
            utility.abort('Query failed to execute.', (' '.join([query, str(list(args))]), e))

    def iter_tables(self):
        for table_name in self.table_names:
            yield self.get_table(table_name)

    #=== Private methods.

    def _initialize(self):
        try:
            self.schema = schemaobject.SchemaObject(self.uri)
        except MySQLdb.DatabaseError, e:
            utility.abort('MySQL database connection exception:', ('URI: %s' % self.uri, e))
        num_databases = len(self.schema.databases)
        if num_databases != 1:
            utility.abort('Expect the URI to uniquely identify a single database.',
                          '"%s" returns %d databases.' % (self.uri, num_databases))
        self.database = self.schema.databases[self.schema.databases.keys()[0]]
        for table_name in self.database.tables:
            self.table_names.append(table_name)
            result = self.execute('SELECT count(*) FROM %s' % table_name)
            row_count = result[0]['count(*)']
            self.tables[table_name] = MySQLTable(self.database.tables[table_name], row_count)
        self.table_names.sort()
        self._determine_partitioning()

    def _determine_partitioning(self):
        # Partition around either a user-specified table or the largest row count.
        partition_table = None
        if self.partition_table_name and self.partition_table_name.upper() != 'AUTO':
            # Find the user-specified table schema.
            for table in self.iter_tables():
                if table.name == self.partition_table_name:
                    partition_table = table
                    reason_chosen = ('%s was explicitly specified for partitioning.' % table.name)
                    break
            else:
                utility.abort('Partitioning table "%s" is not in the schema.'
                                    % self.partition_table_name)
        else:
            # Find the largest row count.
            max_row_count = 0
            for table in self.iter_tables():
                if table.row_count > max_row_count:
                    max_row_count = table.row_count
                    partition_table = table
                    reason_chosen = ('%s has the largest row count of %d.'
                                            % (table.name, max_row_count))
        if not partition_table:
            utility.abort('Unable to generate partitioning without one of the following:', (
                          '- A database populated with representative data.',
                          '- A partitioning table specified on the command line.'))
        pkey_column_name = partition_table.primary_key_columns[0].name
        biggest_ptable = PartitionedTable(partition_table, pkey_column_name, reason_chosen)
        self.partitioned_tables[partition_table.name] = biggest_ptable
        # Partition other tables having a foreign key referencing the biggest table.
        for table in self.iter_tables():
            if table.name == partition_table.name:
                continue
            for fk_name in table.schema.foreign_keys:
                fk = table.schema.foreign_keys[fk_name]
                if fk.referenced_table_name != partition_table.name:
                    continue
                # It's a winner if one of the FK reference columns is the
                # referenced table's partition key.
                for icolumn in range(len(fk.columns)):
                    # Check if the referenced column is the partition key.
                    # If it is use the corresponding column in this table.
                    # Assume columns and referenced_columns are synchronized.
                    if fk.referenced_columns[icolumn] == biggest_ptable.pkey_column_name:
                        pkey_column_name = fk.columns[icolumn]
                        reason_chosen = ('%s references partitioned table %s through a '
                                         'foreign key that references the partition key'
                                                % (table.name, biggest_ptable.name))
                        fk_ptable = PartitionedTable(table, pkey_column_name, reason_chosen)
                        self.partitioned_tables[table.name] = fk_ptable
                        break


def convert_type(mysql_type):
    """
    Convert MySQL type to VoltDB type.
    Return (volt_type, message)
    message is a string with information about conversion issues when they occur.
    """
    m = Global.re_type.match(mysql_type)
    if not m:
        return 'VARCHAR(50)', MESSAGES.PARSE_ERROR
    # Normalize compound names with arbitrary spacing,
    # e.g. "DOUBLE   PRECISION".
    name = ''.join(m.group(1).upper().split())
    param = m.group(3)
    # Generate the return pair based on the presence of an unsupported parameter.
    def noparam(volt_type, message):
        if param:
            return volt_type, message
        return volt_type, None
    if name == 'TINYINT':
        return noparam('TINYINT', MESSAGES.WIDTH_PARAM)
    if name == 'SMALLINT':
        return noparam('SMALLINT', MESSAGES.WIDTH_PARAM)
    if name == 'MEDIUMINT':
        return 'INTEGER', MESSAGES.MEDIUM_INT
    if name == 'INT':
        return noparam('INTEGER', MESSAGES.WIDTH_PARAM)
    if name == 'BIGINT':
        return noparam('BIGINT', MESSAGES.WIDTH_PARAM)
    if name in ('DECIMAL', 'NUMERIC'):
        return noparam('DECIMAL', MESSAGES.PREC_PARAMS)
    if name in ('FLOAT', 'REAL', 'DOUBLE', 'DOUBLE PRECISION'):
        return noparam('FLOAT', MESSAGES.PREC_PARAMS)
    if name == 'BIT':
        nbytes = (int(param) + 7) // 8
        return 'VARBINARY(%d)' % nbytes, MESSAGES.BIT_TYPE
    if name  == 'TIMESTAMP':
        return 'TIMESTAMP', None
    if name in ('DATE', 'DATETIME', 'TIME', 'YEAR'):
        return 'TIMESTAMP', MESSAGES.DATE_TIME
    if name == 'CHAR':
        return 'VARCHAR(%d)' % int(param), MESSAGES.CHAR
    if name == 'VARCHAR':
        return 'VARCHAR(%d)' % int(param), None
    if name == 'ENUM':
        return 'VARCHAR(50)', MESSAGES.ENUM
    if name == 'SET':
        return 'VARCHAR(50)', MESSAGES.SET
    if name in ('TEXT', 'TINYTEXT', 'MEDIUMTEXT', 'LONGTEXT'):
        return 'VARCHAR(1024)', MESSAGES.TEXT
    if name == 'BINARY':
        return 'VARBINARY(%d)' % int(param), MESSAGES.BINARY
    if name == 'VARBINARY':
        return 'VARBINARY(%d)' % int(param), None
    if name in ('BLOB', 'TINYBLOB', 'MEDIUMBLOB', 'LONGBLOB'):
        return 'VARBINARY(1024)', MESSAGES.BLOB
    # Default to varchar and BAD as the result code in case of failure.
    return 'VARCHAR(50)', MESSAGES.UNSUPPORTED
