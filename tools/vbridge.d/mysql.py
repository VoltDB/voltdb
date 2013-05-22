# This file is part of VoltDB.
# Copyright (C) 2008-2013 VoltDB Inc.
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
import re
from voltcli import utility

# A common OS X mysql installation problem can prevent _mysql.so from loading.
# Postpone database imports until initialize() is called to avoid errors for
# in non-mysql commands and to provide good messages for handle import errors.
schemaobject = None
MySQLdb = None

# Regular expression to parse a MySQL type.
re_type = re.compile(r'^\s*\b([a-z0-9_ ]+)\b\s*([(]([^)]*)[)])?\s*$')

# The suggestion to display when OS X mysql module import fails.
osx_fix = 'ln -s /usr/local/mysql/lib/libmysqlclient.[0-9][0-9]*.dylib /usr/local/lib/'

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
)


# Perform the delayed MySQL module imports.
def initialize():
    try:
        import schemaobject as schemaobject_
        import MySQLdb as MySQLdb_
        global schemaobject, MySQLdb
        schemaobject = schemaobject_
        MySQLdb = MySQLdb_
    except ImportError, e:
        msgs = [
            'Failed to import MySQL-related module.',
            'Is MySQL properly installed?',
                ('http://dev.mysql.com/downloads/',)
        ]
        if sys.platform == 'darwin':
            msgs.extend([
                'On OS X you may need to create a symlink, e.g.',
                    (osx_fix,)
            ])
        msgs.append(e)
        utility.abort(*msgs)


@VOLT.Command(
    description='Access MySQL database schema.',
    arguments=(
        VOLT.StringArgument('uri', help='Database URI.')
    )
)
def mysql_schema(runner):
    initialize()
    try:
        schema  = schemaobject.SchemaObject(runner.opts.uri)
    except MySQLdb.DatabaseError, e:
        runner.abort('MySQL database connection exception:', (
                        'URI: %s' % runner.opts.uri, e))
    if len(schema.databases) != 1:
        runner.abort('Expect the URI to uniquely identify a single database.',
                     '"%s" returns %d databases.' % (runner.opts.uri, len(schema.databases)))
    database = schema.databases[schema.databases.keys()[0]]
    write_schema(database.tables, sys.stdout)


def option_value(obj, name):
    return obj.options.get(name, '').value


class OutputSchemaLines(object):
    def __init__(self):
        self.lines = []
        self.last_comma_line = -1
    def add(self, needs_comma, *lines):
        if needs_comma and self.last_comma_line >= 0:
            self.lines[self.last_comma_line] += ','
            self.last_comma_line = -1
        for line in lines:
            if isinstance(line, OutputSchemaLines):
                self.lines.extend(line.lines)
            else:
                self.lines.append(line)
        if needs_comma:
            self.last_comma_line = len(self.lines) - 1
    def __str__(self):
        return '\n'.join(self.lines)


def create_table(table, messages):
    table_comment = option_value(table, 'comment')
    lines = []
    if table_comment:
        lines.append('-- %s' % table_comment)
    lines = OutputSchemaLines()
    lines.add(False, 'CREATE TABLE %s' % table.name, '(')
    primary_key_columns = []
    for column_name in table.columns:
        column = table.columns[column_name]
        if column.comment:
            lines.add(False, '   -- %s' % column.comment)
        if column.key == 'PRI':
            primary_key_columns.append(column)
        voltdb_type, message_number = convert_type(column.type)
        if message_number:
            try:
                n = messages.index(message_number) + 1
            except ValueError:
                messages.append(message_number)
                n = len(messages)
            lines.add(False, '   -- see note (%d)' % n)
        if column.null:
            voltdb_null = ''
        else:
            voltdb_null = ' NOT NULL'
        lines.add(True, '   %s %s%s' % (column.name, voltdb_type, voltdb_null))
    if primary_key_columns:
        constraint_lines = OutputSchemaLines()
        constraint_lines.add(False, '   CONSTRAINT PK_%s PRIMARY KEY' % table.name, '   (')
        for primary_key_column in primary_key_columns:
            constraint_lines.add(True, '      %s' % primary_key_column.name)
        constraint_lines.add(False, '   )')
        lines.add(True, constraint_lines)
    lines.add(False, ');')
    return str(lines)


def write_schema(tables, f):
    messages = []
    for table_name in tables:
        write_schema_table(tables[table_name], f, messages)
        f.write('\n')
    if messages:
        f.write('-- ::: Notes :::\n')
        for i in range(len(messages)):
            f.write('-- (%d) %s\n' % (i + 1, MESSAGES[messages[i]]))


def write_schema_table(table, f, messages):
    f.write('%s\n' % create_table(table, messages))


def convert_type(mysql_type):
    """
    Convert MySQL type to VoltDB type.
    Return (volt_type, message)
    message is a string with information about conversion issues when they occur.
    """
    m = re_type.match(mysql_type)
    if not m:
        return 'VARCHAR(50)', 'Unable to parse'
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
