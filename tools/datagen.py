#! /usr/bin/python3

# Simple datagen tool that generates INSERT queries given the table schema.
# Usage:
# % tools/datagen.py 'CREATE TABLE t(i INTEGER, j FLOAT NOT NULL, k VARCHAR(8), l DECIMAL(8, 3), t TIMESTAMP);' 500
# will generate 500 INSERT statements that you can just pipe to sqlcmd.
#
# Useful for populating quick data into tables for customer issues.
import re, string, sys, random

def failwith(msg, retcode=2):
    print(msg)
    sys.exit(2)

column_types = {
    'INT' : lambda _ : lambda:random.randint(-9999,9999),
    'INTEGER' : lambda _ : lambda:random.randint(-9999,9999),
    'SMALLINT' : lambda _ : lambda:random.randint(-9999, 9999),
    'TINYINT' : lambda _ : lambda:random.randint(-127,128),
    'BIGINT' : lambda _ : lambda:random.randint(-9999, 9999),
    'FLOAT' : lambda _ : lambda:random.uniform(-9999, 9999),
    'DECIMAL' : lambda width : lambda:random.randint(-10**(width-1), 10**(width-1)),
    'VARCHAR' : lambda width : lambda:
    "'" + ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
                  for _ in range(random.randint(1, width))) + "'",
    'TIMESTAMP': lambda _ :lambda:'TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, CURRENT_TIMESTAMP())%+d)' % random.randint(-9999, 9999),

    'GEOGRAPHY': lambda _ :lambda:failwith('GEOGRAPHY unsupported'),
    'GEOGRAPHY_POINT': lambda  _ :lambda:failwith('GEOGRAPHY_POINT unsupported'),
}

def parse_table_field(field):
    elms = re.compile(' +').split(field)
    assert len(elms) > 1
    elms = elms[1].split('(')
    ctype = elms[0]  # ignore width/precision

    if ctype not in column_types:
        failwith('Unknown column type parsed: %s' % ctype)
    else:
        if len(elms) > 1:
            width = int(re.compile('[,)]').split(elms[1])[0])
        else:
            width = 0
        return column_types[ctype](width)

def datagen(fn, number):
    return tuple(fn() for _ in range(number))

def main(sql, rows):
    tokens = [i.upper() for i in re.compile(' +').split(sql) if i != '']
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
        columns = [datagen(fn, rows) for fn in [parse_table_field(i) for i in re.compile(', +').split(' '.join(tokens))]]
        columns = [list(i) for i in list(zip(*columns))]
        columns = [', '.join([str(s) for s in i]) for i in columns]
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
