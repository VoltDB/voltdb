#! /usr/bin/python3

# Simple datagen tool that generates INSERT queries given the table schema.
# Usage:
# % tools/datagen.py 'CREATE TABLE t(i INTEGER NOT NULL, j FLOAT NOT NULL, k VARCHAR(8), l DECIMAL(8,3), t TIMESTAMP, primary key (i));' 500
# will generate 500 INSERT statements that you can pipe to sqlcmd.
#
# Useful for populating quick data into tables for customer issues.

import re, string, sys, random

def rand_char(len, src=string.ascii_letters + string.digits):
    """
    Generate random characther of given maximum length.
    """
    # No escape needed inside the generated content.
    return "'%s'" % \
            (''.join(random.choice(src) for _ in range(random.randint(1, len))))

def failwith(msg, retcode=2):
    sys.stderr.write('datagen error: %s' % msg)
    sys.exit(retcode)

# constants for regex
RE_WS = re.compile(' +')
RE_FIELD_SEP = re.compile(', +')
RE_COMMA_OR_RIGHT_PAREN = re.compile('[,)]')

# constants for constraint keywords and datagen functions
CONSTRAINT_KEYWORDS = set(['CONSTRAINT', 'PRIMARY', 'UNIQUE', 'ASSUMEUNIQUE'])
COLUMN_TYPES = {
    'INT' : lambda _ : lambda:random.randint(-9999,9999),
    'INTEGER' : lambda _ : lambda:random.randint(-9999,9999),
    'SMALLINT' : lambda _ : lambda:random.randint(-9999, 9999),
    'TINYINT' : lambda _ : lambda:random.randint(-128,127),
    'BIGINT' : lambda _ : lambda:random.randint(-9999, 9999),
    'FLOAT' : lambda _ : lambda:random.uniform(-9999, 9999),
    'DECIMAL' : lambda width : lambda:random.randint(-10**(width-1), 10**(width-1)),       #  ignore precision
    'VARCHAR' : lambda width : lambda:rand_char(width),
    'VARBINARY' : lambda _ : lambda:failwith('VARBINARY unsupported'),
    # TODO: this is buggy; what Volt allows as VARBINARY is also limiting (and undocumented?).
    #"CAST(%s AS VARBINARY(%d))" % (rand_char(width, string.hexdigits), width),
    'TIMESTAMP': lambda _ :lambda:'TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, CURRENT_TIMESTAMP())%+d)' % random.randint(-9999, 9999),
    'GEOGRAPHY': lambda _ :lambda:failwith('GEOGRAPHY unsupported'),
    'GEOGRAPHY_POINT': lambda  _ :lambda:failwith('GEOGRAPHY_POINT unsupported'),
}

def parse_table_field(field):
    elms = RE_WS.split(field)
    assert len(elms) > 1
    varname = elms[0]
    elms = elms[1].split('(')
    ctype = elms[0]
    if varname in CONSTRAINT_KEYWORDS:                                 #  either a constraint or a column decl
        return None
    elif ctype not in COLUMN_TYPES:
        failwith('Unknown column type parsed: %s' % ctype)
    else:
        return COLUMN_TYPES[ctype](int(RE_COMMA_OR_RIGHT_PAREN.split(elms[1])[0]) if len(elms) > 1 else 64)

def datagen(fn, number):
    return tuple(fn() for _ in range(number))

def main(sql, rows):
    tokens = [i.upper() for i in RE_WS.split(sql) if i != '']
    if len(tokens) < 3 or tokens[0] != 'CREATE' or tokens[1] != 'TABLE' or \
       tokens[2].find('(') < 0 or not tokens[-1].endswith(');'):
        failwith('%s is invalid CREATE TABLE statement.\nNo space allowed between parenthesis and table name/semicolon' % sql, 1)
    else:
        tokens = tokens[2:]
        tmp = tokens[0].split('(')
        assert len(tmp) == 2
        name = tmp[0]
        tokens[0] = tmp[1]
        tokens[-1] = tokens[-1][0:-2]
        columns = [datagen(fn, rows) for fn in [parse_table_field(i) for i in RE_FIELD_SEP.split(' '.join(tokens))] if fn is not None]     # datagen for each field
        columns = [list(i) for i in list(zip(*columns))]            # transpose
        columns = [', '.join([str(s) for s in i]) for i in columns] # join fields
        return '\n'.join(['INSERT INTO %s VALUES(%s);' % (name, c) for c in columns])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        failwith("""
                 Usage: datagen.py 'create table foo(i int, j float, k varchar(256));' 10
                 Generates 10 insert statements for table foo.
                 No whitespaces allowed around parenthesis. KISS.
                 """)
    else:
        print(main(sys.argv[1], int(sys.argv[2])))
        sys.exit(0)
