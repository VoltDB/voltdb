#!/usr/bin/env python

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

import sys, os
from xml.etree.ElementTree import ElementTree

root_dir = os.path.dirname(os.path.dirname(__file__))
config_path = os.path.join(root_dir, 'config.xml')
project_path = os.path.join(root_dir, 'project.xml')
ddl_path = os.path.join(root_dir, 'ddl.sql')

ddl_start = '-- DDL generated for ad hoc benchmark - this will be overwritten'
ddl_end = None
ddl_pk = 'CONSTRAINT PK_%s PRIMARY KEY (%s)'
ddl_partition = '-- PARTITION BY ( %s )'
table_start = '\nCREATE TABLE %s\n('
table_end = ');'
project_start = '''\
<?xml version="1.0"?>
<project>
    <info>
        <name>Ad Hoc Benchmark</name>
        <version>1.0</version>
        <description>Analyzes ad hoc query overhead.</description>
    </info>
    <database>
        <schemas>
            <schema path='ddl.sql' />
        </schemas>
        <partitions>'''
project_partition="            <partition table='%s' column='%s' />"
project_end = '''\
        </partitions>
    </database>
</project>'''
announcement_separator = '-' * 42

class Fatal(Exception):
    def __init__(self, *msgs):
        self.msgs = msgs
        Exception.__init__(self)

def display(tag, *msgs):
    for msg in msgs:
        if tag:
            sys.stderr.write('%s: ' % tag)
            sys.stderr.write(str(msg))
            sys.stderr.write('\n')

def announce(*msgs):
    print(announcement_separator)
    for msg in msgs:
        print(msg)
    print(announcement_separator)

class Column(object):
    def __init__(self, name, type, modifiers):
        self.name = name
        self.type = type
        self.modifiers = modifiers

class Table(object):
    def __init__(self, name, nvariations, columns, ipartcol, ipkcol):
        self.name = name
        self.nvariations = nvariations
        self.columns = columns
        self.partcol = ipartcol
        self.pkcol = ipkcol

def create_table(name, prefix, nvariations, ncolumns, ipartcol, ipkcol):
    # Add a possible primary / possible partition key and a foreign/non-partition key.
    columns = [Column('id', 'bigint', 'NOT NULL'), Column('parent_id', 'bigint', None)]
    for i in range(ncolumns):
        columns.append(Column('%s_%d' % (prefix, i+1), 'varchar(32)', None))
    return Table(name, nvariations, columns, ipartcol, ipkcol)

def get_tables():
    print 'Reading %s...' % config_path
    tables = []
    et = ElementTree()
    try:
        xmlschema = et.parse(config_path).find('schema')
        if xmlschema is None:
            raise Fatal('Missing <schema> element')
        xmltables = xmlschema.findall('table')
        if xmltables is None or len(xmltables) == 0:
            raise Fatal('No <table> elements found')
        for xmltable in xmltables:
            name = xmltable.attrib['name']
            if not name:
                raise Fatal('Table name is required')
            prefix = xmltable.attrib['prefix']
            if not prefix:
                raise Fatal('Column prefix is required')
            nvariations = int(xmltable.attrib.get('variations', 1))
            if nvariations < 1:
                raise Fatal('Bad variations value: %d' % nvariations)
            ncolumns = int(xmltable.attrib['columns'])
            if ncolumns < 1:
                raise Fatal('Bad columns value: %d' % ncolumns)
            try:
                ipartcol = int(xmltable.attrib['partitioncolumn'])
                if ipartcol < 0 or ipartcol >= ncolumns:
                    raise Fatal('Bad partition column index value: %d' % ipartcol)
            except KeyError, e:
                ipartcol = None # The partitioncolumn attribute is optional, tables default to replicated
            try:
                ipkcol = int(xmltable.attrib['primarykey'])
                if ipkcol < 0 or ipkcol >= ncolumns:
                    raise Fatal('Bad primary key column index value: %d' % ipkcol)
            except KeyError, e:
                ipkcol = None # The primarykey attribute is optional, tables default to having no pk index
            tables.append(create_table(name, prefix, nvariations, ncolumns, ipartcol, ipkcol))
    except (OSError, IOError), e:
        raise Fatal('Failed to parse %s' % config_path, e)
    except KeyError, e:
        raise Fatal('Missing table attribute', e)
    except ValueError, e:
        raise Fatal('Bad table attribute value', e)
    return tables

def generate_project(tables):
    yield project_start
    for table in tables:
        if table.partcol is not None:
            for variation in range(table.nvariations):
                yield project_partition % ('%s_%d' % (table.name, variation+1),
                                                      table.columns[table.partcol].name)
    yield project_end

def generate_comma_separated_list(generator, indent, comment):
    first = True
    for line in generator:
        if first:
            preamble = indent
            first = False
        elif comment is not None and line.startswith(comment):
            preamble = indent
        else:
            preamble = ',%s' % indent[1:]
        yield '%s%s' % (preamble, line)

def generate_table_ddl_lines(table, name):
    for column in table.columns:
        if column.modifiers:
            modifiers = ' %s' % column.modifiers
        else:
            modifiers = ''
        yield '%s %s%s' % (column.name, column.type, modifiers)
    if table.pkcol is not None:
        yield ddl_pk % (name, table.columns[table.pkcol].name)
    if table.partcol is not None:
        yield ddl_partition % (table.columns[table.partcol].name)

def generate_ddl(tables):
    yield ddl_start
    for table in tables:
        for variation in range(table.nvariations):
            name = '%s_%d' % (table.name, variation + 1)
            yield table_start % name
            generator = generate_table_ddl_lines(table, name)
            for line in generate_comma_separated_list(generator, '  ', '--'):
                yield line
            yield table_end
    yield ddl_end

def generate_file(path, generator):
    print 'Generating %s...' % path
    try:
        try:
            f = open(path, 'w')
        except (OSError, IOError), e:
            raise Fatal('Failed to open %s' % path, e)
        for text_block in generator:
            if text_block is not None:
                try:
                    f.write('%s\n' % text_block)
                except (OSError, IOError), e:
                    raise Fatal('Failed to write to %s' % path, e)
    finally:
        f.close()

def main():
    tables = get_tables()
    generate_file(project_path, generate_project(tables))
    generate_file(ddl_path, generate_ddl(tables))

if __name__ == '__main__':
    try:
        main()
        announce('Schema/project generation succeeded.')
    except Fatal, e:
        display('FATAL', *e.msgs)
        announce('Schema/project generation failed.')
        sys.exit(1)
