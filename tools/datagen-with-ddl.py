#! /usr/bin/python3

# Simple datagen tool that garnish a DDL file to populate all tables created
# Usage:
# % tools/datagen.py ddl.sql 500
# will generate 500 INSERT statements for each table created in the ddl.sql
#
# Useful for populating quick data into tables for customer issues.

import datagen, itertools, re, sys

# constants for regex
RE_WS = re.compile(' +')
RE_NON_WS = re.compile('\\S')
RE_TRAILING_WS = re.compile(' *$')
RE_LEADING_WS = re.compile('^ *')
RE_BLOCK_COMMENT = re.compile('/\*.*\*/', re.S)

def context(content, startpos, endpos = None, width = 16):
    """
    Get context as [startpos - width, endpos + width] from the given content
    """
    len = len(content)
    if endpos is None:
        endpos = startpos
    assert startpos >= 0 and len > startpos
    assert endpos >= 0 and len > endpos
    assert startpos <= endpos
    return content[max(0, startpos - width) : min(len - 1, endpos +  width)]

def strip_line_comment(line):
    """
    Strip SQL line-style comment from current line: ... -- some comments
    TODO: quoted "--" are still treated as comment.
    """
    endpos = line.find('--')
    return re.sub(RE_TRAILING_WS, '', \
                  line[RE_NON_WS.search(line).end(0) - 1 : (endpos if endpos >= 0 else len(line))]) if line != '' \
            else line

def strip_block_comment(lines):
    """
    Strip SQL block-style comment from the lines:
        ... /* some multi-
        line comments
        that can be quite long
        */;
    TODO: quoted /* or */ are still treated as comment.
    """
    return re.sub(RE_BLOCK_COMMENT, '', lines)


def strip(content):
    """
    Strip DDL content from all comments and blank lines.
    Return a list of processed lines.
    """
    return [line for line in map(strip_line_comment, strip_block_comment(''.join(content)).split('\n')) if RE_WS.fullmatch(line) is None]

def compact(lines):
    """
    Compact so that each statement takes a single line.
    Must be done after comments are removed.
    Returns a list of compacted lines
    TODO: semicolumns in quoted strings are treated as end of statement.
    """
    return ';\n'.join([re.sub(RE_LEADING_WS, '', line) for line in ' '.join(lines).split(';')]).split('\n')

class StatefulDatagen():
    """
    The datagen process needs to be stateful, because INSERT statement should only be inserted
    after PARTITION statement, if there is any.
    The actual place of insertion is right before each next CREATE TABLE statement, since some indexes also
    require table to be empty at the time of creation...
    """
    def __init__(self, num_inserts):
        self.current_table = None
        self.current_create_table_stmt = None
        self.buffered_statements = []
        self.num_inserts = num_inserts

    def __populate_table__(self):
        if self.current_create_table_stmt is not None:
            self.buffered_statements.extend(datagen.main(self.current_create_table_stmt, self.num_inserts).split('\n'))    # populate previous table

    def produce(self):     # each object can only call produce() at most once
        if self.buffered_statements is None:
            failwith("StatefulDatagen.produce() already drained")
        else:
            self.__populate_table__()
        return self.buffered_statements

    def consume(self, line):
        """
        Check whether current statement line is CREATE TABLE statement.
        If it is, append with given number of INSERT statements; otherwise return as is.
        Assume that there is no leading ws in the line.
        """
        line_copy = [line]
        l = list(re.split(RE_WS, line.upper()))
        if len(l) > 2 and l[0] == 'CREATE' and l[1] == 'TABLE':
            tablename = l[2]
            if '(' in tablename:
                tablename = l[0 : tablename.index('(')]
            self.current_table = tablename
            if self.current_create_table_stmt is not None:
                self.__populate_table__()    # populate last table created
            self.current_create_table_stmt = line
        self.buffered_statements.append(line)

def garnish(lines, num_inserts):
    """
    Garnish a list of lines of DDL statements that had been sanitized (comments/empty lines squeezed),
    with given number of INSERT INTO tbl statements at appropriate locations.
    """
    gen = StatefulDatagen(num_inserts)
    for line in lines:
        gen.consume(line)
    return gen.produce()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        datagen.failwith("""
                         Usage: data-gen-with-ddl.py example.ddl 50
                         Will generate DDL mixed with INSERT statements, each table created there would have 50 tuples.
                         """)
    else:
        with open(sys.argv[1], 'r') as fd:
            print('\n'.join(garnish(compact(strip(fd.readlines())), int(sys.argv[2]))))
