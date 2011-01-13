#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2011 VoltDB Inc.
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

import decimal
import os.path
import re
import random
import time
import datetime
from sys import maxint
from voltdbclient import *

VARIABLE_TYPE = "_variable"
VALUE_TYPE = "_value"
TABLE_TYPE = "_table"

COUNT = 2                       # number of random values to generate by default
IS_VOLT = False

# Python 2.4 doesn't have these two methods in the itertools module, so here's
# the equivalent implementations.
def product(*args, **kwds):
    # product('ABCD', 'xy') --> Ax Ay Bx By Cx Cy Dx Dy
    # product(range(2), repeat=3) --> 000 001 010 011 100 101 110 111
    pools = map(tuple, args) * kwds.get("repeat", 1)
    result = [[]]
    for pool in pools:
        result = [x + [y] for x in result for y in pool]
    return result

def permutations(iterable, r = None):
    pool = tuple(iterable)
    n = len(pool)
    r = (r is None) and n or r
    for indices in product(range(n), repeat=r):
        if len(set(indices)) == r:
            yield [pool[i] for i in indices]

def field_name_generator():
    i = 0
    while True:
        yield "field_%d" % (i)
        i += 1
fn_generator = field_name_generator()

def map_type(type_str):
    TYPES = {"int": (FastSerializer.VOLTTYPE_TINYINT,
                     FastSerializer.VOLTTYPE_SMALLINT,
                     FastSerializer.VOLTTYPE_INTEGER,
                     FastSerializer.VOLTTYPE_BIGINT),
             "byte": ((FastSerializer.VOLTTYPE_TINYINT),),
             "int16": ((FastSerializer.VOLTTYPE_SMALLINT),),
             "int32": ((FastSerializer.VOLTTYPE_INTEGER),),
             "int64": ((FastSerializer.VOLTTYPE_BIGINT),),
             "float": ((FastSerializer.VOLTTYPE_FLOAT),),
             "string": ((FastSerializer.VOLTTYPE_STRING),),
             "decimal": ((FastSerializer.VOLTTYPE_DECIMAL),),
             "date": ((FastSerializer.VOLTTYPE_TIMESTAMP),)}

    if type_str in TYPES:
        return TYPES[type_str]
    else:
        return None

class BaseValueGenerator:
    """This is the base class for all value generators (int, float, etc.)
    """

    def __init__(self):
        pass

    def generate(self, count):
        pass

    def generate_boundries(self):
        pass

class IntValueGenerator(BaseValueGenerator):
    """This is the base generator for integers.
    """

    def __init__(self, min = None, max = None):
        BaseValueGenerator.__init__(self)

        self.__min = min == None and (-maxint - 1) or min
        self.__max = max == None and maxint or max

    def generate(self, count):
        values = [random.randint(self.__min, self.__max)
                  for i in xrange(count)]
        return values

    def generate_boundries(self):
        return [self.__min, self.__max]

class IDValueGenerator(BaseValueGenerator):
    """This generates unique incremental integers.
    """

    counter = 0

    def __init__(self, start = None):
        BaseValueGenerator.__init__(self)

        if start != None:
            self.__class__.counter = start

    def generate(self, count):
        values = []

        for i in xrange(count):
            values.append(self.__class__.counter)
            self.__class__.counter += 1

        return values

class ByteValueGenerator(IntValueGenerator):
    """This generates bytes.
    """

    def __init__(self):
        IntValueGenerator.__init__(self, -127, 127)

class Int16ValueGenerator(IntValueGenerator):
    """This generates 16-bit integers.
    """

    def __init__(self):
        IntValueGenerator.__init__(self, -32767, 32767)

class Int32ValueGenerator(IntValueGenerator):
    """This generates 32-bit integers.
    """

    def __init__(self):
        IntValueGenerator.__init__(self, -2147483647, 2147483647)

class Int64ValueGenerator(IntValueGenerator):
    """This generates 64-bit integers.
    """

    def __init__(self):
        IntValueGenerator.__init__(self, -9223372036854775807,
                                   9223372036854775807)

class FloatValueGenerator(BaseValueGenerator):
    """This generates 64-bit float.
    """

    def __init__(self):
        BaseValueGenerator.__init__(self)

    def generate(self, count):
        values = [random.random()
                  for i in xrange(count)]
        return values

    def generate_boundries(self):
        return [float("-inf"), float("nan")]

class DecimalValueGenerator(BaseValueGenerator):
    """This generates Decimal values.
    """

    def __init__(self):
        BaseValueGenerator.__init__(self)
        # currently we only support 12 digits of precision
        decimal.getcontext().prec = 12

    def generate(self, count):
        # we support 7 digits of scale, so magnify those tiny floats
        values = [decimal.Decimal(str(random.random() * 1e2))
                  for i in xrange(count)]
        return values

    def generate_boundries(self):
        # fill me in with something real if we ever are actually called
        pass

class StringValueGenerator(BaseValueGenerator):
    """This generates strings.
    """

    def __init__(self):
        BaseValueGenerator.__init__(self)

    def generate(self, count, length = 17):
        alphabet = u"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        values = []
        for i in xrange(count):
            string = u""
            for y in xrange(length):
                string += random.choice(alphabet)
            values.append(string)
        return values

class DateValueGenerator(BaseValueGenerator):
    """This generates dates.
    """

    def __init__(self):
        BaseValueGenerator.__init__(self)

    def generate(self, count):
        # HSQL doesn't support microsecond, 13 digit number
        max_dt = 9999999999999
        values = [random.randint(0, max_dt) for i in xrange(count)]

        return values

class BaseGenerator:
    """This is the base class for all generators (operator generator, column
    generator, etc.).
    """

    def __init__(self, token):
        global fn_generator

        self.__token = token
        self.fn_gen = fn_generator

    def generate(self, statement, params):
        """All derived classes should overload this method to implement their
        own generators.
        """

        pass

    def get_token(self):
        return self.__token

    def prepare_operands(self, statement):
        pass

class VariableGenerator(BaseGenerator):
    """This replaces variables with specific names.
    """

    __EXPR_TEMPLATE = r"%s(\[%s(\w+)?(:(((\d+),(\d+))|(\w+)))?\])%s"

    def __init__(self, operand_type, label = None):
        BaseGenerator.__init__(self, None)

        self.EXPR = self._expr_builder(operand_type, label)

        self.__label = None
        self.__type = None
        self._fn = None

    @classmethod
    def _expr_builder(cls, operand_type, label = None, must_exist = False):
        if label:
            label = "(@(%s)\s*)" % (label)
            must_exist = True
        else:
            label = "(@(\w+)\s*)?"
            must_exist = False

        if must_exist:
            tail = ""
        else:
            tail = "?"
        return re.compile(cls.__EXPR_TEMPLATE % (operand_type, label, tail))

    def get_label(self):
        return self.__label

    def get_type(self):
        return self.__type

    def generate(self, statement, params):
        """params is a list of variable name.
        """

        for i in params:
            yield statement.replace(self._fn, i, 1)

    def prepare_operands(self, statement):
        result = self.EXPR.search(statement)

        if result:
            self.__label = result.group(3)
            self.__type = result.group(4)
            self._fn = "{%s}" % (self.fn_gen.next())
            stmt = self.EXPR.sub(self._fn, statement, 1)
        else:
            return None

        return [stmt, [self]]

class SingletonOpGenerator(BaseGenerator):
    """This is the base generator for all singleton operator generators.
    """

    def __init__(self, ops, token = "_singleton"):
        BaseGenerator.__init__(self, token)

        self.EXPR = re.compile(r"%s" % (token))

        self.__ops = ops
        self.__fn = None

    def generate(self, statement, params):
        """params is a dummy variable.
        """

        for op in self.__ops:
            yield statement.replace(self.__fn, op, 1)

    def prepare_operands(self, statement):
        result = self.EXPR.search(statement)

        if result:
            self.__fn = "{%s}" % (self.fn_gen.next())
            stmt = self.EXPR.sub(self.__fn, statement, 1)
        else:
            return None

        return [stmt, [None]]

class BinaryOpGenerator(BaseGenerator):
    """This is the base generator for all binary operator generators.
    """

    def __init__(self, ops, token):
        BaseGenerator.__init__(self, token)

        self.__ops = ops
        self.__op_fn = None
        self.__l = None
        self.__r = None

    def generate(self, statement, params):
        """params need to be a list of (left, right) pairs.
        """

        for op in self.__ops:
            stmt = statement.replace(self.__op_fn, op, 1)
            for i in params:
                if self.__l != None:
                    for l in self.__l.generate(stmt, (i[0],)):
                        if self.__r != None:
                            for r in self.__r.generate(l, (i[1],)):
                                yield r
                        else:
                            yield l
                else:
                    if self.__r != None:
                        for r in self.__r.generate(stmt, (i[1],)):
                            yield r
                    else:
                        yield stmt

    def prepare_operands(self, statement):
        tokens = statement.split()
        index = tokens.index(self.get_token())
        result = [None, None]

        self.__op_fn = "{%s}" % (self.fn_gen.next())
        tokens[index] = self.__op_fn

        if index > 0:
            self.__l = VarGenerator()
            tmp = self.__l.prepare_operands(tokens[index - 1])
            if tmp != None:
                tokens[index - 1] = tmp[0]
                result[0] = self.__l
            else:
                self.__l = None

        if index < len(tokens) - 1:
            self.__r = VarGenerator()
            tmp = self.__r.prepare_operands(tokens[index + 1])
            if tmp != None:
                tokens[index + 1] = tmp[0]
                result[1] = self.__r
            else:
                self.__r = None

        return [" ".join(tokens), result]

class AggregationGenerator(BaseGenerator):
    """This generates statements with aggregation functions.
    """

    def __init__(self):
        BaseGenerator.__init__(self, "_agg")

        self.__ops = ("SUM", "AVG", "MIN", "MAX", "COUNT")
        self.__op_fn = None
        self.__operand = None

    def generate(self, statement, params):
        """params need to be a list of expressions.
        """

        for op in self.__ops:
            stmt = statement.replace(self.__op_fn, op, 1)
            if self.__operand != None:
                for s in self.__operand.generate(stmt, params[0]):
                    yield s
            else:
                yield stmt

    def prepare_operands(self, statement):
        tokens = statement.split()

        for i in xrange(len(tokens)):
            if (self.get_token() in tokens[i]):
                self.__op_fn = "{%s}" % (self.fn_gen.next())
                tokens[i] = tokens[i].replace(self.get_token(),
                                              self.__op_fn, 1)

                self.__operand = VarGenerator()
                tmp = self.__operand.prepare_operands(tokens[i])
                if tmp != None:
                    tokens[i] = tmp[0]
                    return [" ".join(tokens), [self.__operand]]

                break

        return [" ".join(tokens), [None]]

class TableGenerator(VariableGenerator):
    """This replaces variable with table name.
    """

    def __init__(self):
        VariableGenerator.__init__(self, None)
        self.EXPR = re.compile(r"%s" % (TABLE_TYPE))

        self.__type = None
        self.__fn = []

    def get_type(self):
        return self.__type

    def generate(self, statement, params):
        """params is a list of variable name.
        """

        stmt = statement
        if len(self.__fn) == len(params):
            for k, v in zip(self.__fn, params):
                stmt = stmt.replace(k, v, 1)
            yield stmt
        elif len(self.__fn) < len(params):
            for i in xrange(0, len(params), len(self.__fn)):
                stmt = statement
                for k, v in zip(self.__fn, params[i:i + len(self.__fn)]):
                    stmt = stmt.replace(k, v, 1)
                yield stmt

    def prepare_operands(self, statement):
        stmt = statement
        result = self.EXPR.search(stmt)

        while result:
            self.__fn.append("{%s}" % (self.fn_gen.next()))
            stmt = self.EXPR.sub(self.__fn[-1], stmt, 1)
            result = self.EXPR.search(stmt)

        if stmt == statement:
            return None

        return [stmt, [self]]

class VarGenerator(VariableGenerator):
    """This replaces variable with table name.
    """

    def __init__(self):
        VariableGenerator.__init__(self, VARIABLE_TYPE)

        self.__cnt = 1

    def generate(self, statement, params):
        """params is a list of variable name.
        """

        for i in params:
            yield statement.replace(self._fn, i, self.__cnt)

    def prepare_operands(self, statement):
        result = self.EXPR.search(statement)

        if result:
            self.__label = result.group(3)
            self.__type = result.group(4)
            self._fn = "{%s}" % (self.fn_gen.next())

            label = self.__label
            stmt = self.EXPR.sub(self._fn, statement, 1)
            # Search for the variables with the same label and assign the same
            # place holder for all of them as if they are the same variable.
            while label:
                self.EXPR = self._expr_builder(VARIABLE_TYPE, label)
                result = self.EXPR.search(stmt)
                if not result:
                    break
                label = result.group(3)
                if label != self.__label:
                    break
                stmt = self.EXPR.sub(self._fn, stmt, 1)
                self.__cnt += 1
        else:
            return None

        return [stmt, [self]]

class ValueGenerator(VariableGenerator):
    """This replaces variable with actual value.
    """

    UNIQUE_TYPES = {"id": IDValueGenerator}
    TYPES = {"int": IntValueGenerator,
             "byte": ByteValueGenerator,
             "int16": Int16ValueGenerator,
             "int32": Int32ValueGenerator,
             "int64": Int64ValueGenerator,
             "float": FloatValueGenerator,
             "string": StringValueGenerator,
             "decimal": DecimalValueGenerator,
             "date": DateValueGenerator}

    def __init__(self):
        VariableGenerator.__init__(self, VALUE_TYPE)

        self.__type = None
        self.__range = None
        self.__fn = None

    def is_unique(self):
        if self.__type in self.__class__.UNIQUE_TYPES:
            return True
        return False

    def generate(self, statement, params):
        """params is a place holder here, not used.
        """

        assert self.__type != None
        if self.__type in self.__class__.TYPES:
            type = self.__class__.TYPES
        else:
            type = self.__class__.UNIQUE_TYPES

        if self.__range != None and None not in self.__range:
            values = type[self.__type](int(self.__range[0]),
                                       int(self.__range[1]))
        else:
            values = type[self.__type]()
        for i in values.generate(COUNT):
            if IS_VOLT and self.__type == "date":
                i = i * 1000
            if isinstance(i, basestring):
                i = u"'%s'" % (i)
            elif isinstance(i, float):
                i = u"%.20e" % (i)
            yield statement.replace(self.__fn, unicode(i), 1)

    def prepare_operands(self, statement):
        result = self.EXPR.search(statement)

        if result:
            self.__type = result.group(4)
            self.__range = result.group(8, 9)
            self.__fn = r"{%s}" % (self.fn_gen.next())
            stmt = self.EXPR.sub(self.__fn, statement, 1)
        else:
            return None

        return [stmt, [self]]

class NegationGenerator(SingletonOpGenerator):
    """This generator generates statements using the not operator.
    """

    def __init__(self):
        SingletonOpGenerator.__init__(self, ("NOT",))

class DistinctGenerator(SingletonOpGenerator):
    """This generator generates statements using the distinct operator.
    """

    def __init__(self):
        SingletonOpGenerator.__init__(self, ("DISTINCT", ""), "_distinct")

class SortOrderGenerator(SingletonOpGenerator):
    """This generator generates sort order ASC, DESC, blank for ORDER BY
    """

    def __init__(self):
        SingletonOpGenerator.__init__(self, ("", "ASC", "DESC"), "_sortorder")

class MathGenerator(BinaryOpGenerator):
    """This generator generates statements using different operators (+, -, *,
    /, %).
    """

    def __init__(self):
        #BinaryOpGenerator.__init__(self, ("+", "-", "*", "/", "%"), "_math")
        BinaryOpGenerator.__init__(self, ("+", "-", "*", "/"), "_math")

class CmpGenerator(BinaryOpGenerator):
    """This generator generates statements using comparison operators (<, >, =,
    etc.).
    """

    def __init__(self):
        BinaryOpGenerator.__init__(self, ("<", ">", "=", "<=", ">=", "!=", "<>"),
                                   "_cmp")

class SetGenerator(BinaryOpGenerator):
    """This generates statements using set operators (union, etc.).
    """

    def __init__(self):
        BinaryOpGenerator.__init__(self, ("UNION",), "_set")

class LogicGenerator(SingletonOpGenerator):
    """This generates statements using logic operators (AND, OR).
    """

    def __init__(self):
        SingletonOpGenerator.__init__(self, ("AND", "OR"), "_logic")
        #SingletonOpGenerator.__init__(self, ("AND",), "_logic")

class Statement:
    def __init__(self, statement, operators, variables, parent = None):
        self.__statement = unicode(statement)
        self.__operators = operators
        self.__variables = variables
        if parent:
            self.__tables = parent.__tables
            self.__table_count = parent.__table_count
            self.__delay = parent.__delay
        else:
            self.__tables = []
            self.__table_count = None
            self.__delay = None # Only one delayed type is supported in a single
                                # statement for now.

    def get_table_count(self):
        if self.__table_count:
            return self.__table_count

        self.__table_count = len(re.findall(TABLE_TYPE, self.__statement))
        return self.__table_count

    def get_statement(self):
        if self.__delay != None:
            for stmt in self.__delay.generate(self.__statement, None):
                yield stmt
        else:
            yield self.__statement

    def get_tables(self):
        return self.__tables

    def set_tables(self, tables):
        if not self.__tables:
            self.__tables = tables

    def next(self):
        for i in self.__operators:
            gen = i()
            if gen.get_token() in self.__statement:
                return [gen, gen.prepare_operands(self.__statement)]

        gen = None

        for i in self.__variables:
            gen = i()
            ret = gen.prepare_operands(self.__statement)
            if ret != None:
                if isinstance(gen, ValueGenerator) and gen.is_unique():
                    self.__statement = ret[0]
                    self.__delay = gen
                    gen = i()
                    ret = gen.prepare_operands(ret[0])
                    if ret != None:
                        return [gen, ret]
                    continue
                elif isinstance(gen, TableGenerator) and not self.__tables:
                    continue
                return [gen, ret]

        gen = TableGenerator()
        ret = gen.prepare_operands(self.__statement)
        if ret != None and not self.__tables:
            return [gen, ret]

        return None

class Schema:
    def __init__(self, **kwargs):
        if 'filename' in kwargs:
            self.__init_from_file(kwargs['filename'])
        elif 'schema' in kwargs:
            self.__schema = kwargs['schema']
        else:
            print "No schema provided"
        self.__types = None

    def __init_from_file(self, filename):
        fd = open(filename, "r")
        self.__content = fd.read()
        fd.close()
        self.__schema = eval(self.__content.strip())

    def get_tables(self):
        return self.__schema.keys()

    def get_indexes(self, table):
        return self.__schema[table]["indexes"]

    def get_partitions(self, table):
        return self.__schema[table]["partitions"]

    def get_columns(self, table):
        return self.__schema[table]["columns"]

    def pick_columns(self, table_count, count, tables, type = None):
        """Returns count number of column names of the same column type.

        If no column type has at least count number of columns, None is returned.
        """

        # calculate the number of columns has to be picked from each table
        col_per_table = count / (table_count == 0 and 1 or table_count)

        if len(self.__schema) < table_count:
            return

        # Filter out the tables with enough columns
        if not tables:
            table_space = filter(lambda x: self.__filter(x, type, col_per_table),
                                 self.__schema.keys())
            try:
                tables.extend(random.sample(table_space, table_count))
            except:
                return

        for t in self.__types:
            candidates = []
            for table in tables:
                tmp = list(map(lambda x: ".".join((table, x[0])),
                               filter(lambda x: x[1] == t and True or False,
                                      self.__schema[table]["columns"])))
                if not tmp:
                    candidates = []
                    break
                candidates.extend(tmp)
            for result in permutations(candidates, count):
                yield result

    def __filter(self, x, type, col_per_table):
        # Find a type that has enough columns to satisfy count
        if type:
            self.__types = [i[1] for i in self.__schema[x]["columns"]
                            if i[1] in type]
        else:
            self.__types = [i[1] for i in self.__schema[x]["columns"]]
        self.__types = set(i for i in self.__types
                           if self.__types.count(i) >= col_per_table)

        if len(self.__types) == 0:
            return False
        return True

class Template:
    def __init__(self, **kwargs):
        self.__lines = []
        self.__dml = None
        self.__query = None
        if 'filename' in kwargs:
            self.__lines = self.__init_from_file(kwargs['filename'])
        elif 'lines' in kwargs:
            self.__lines = kwargs['lines']
        else:
            print "No lines in template, no SQL will be generated"
        self.__lines = self.preprocess(self.__lines)

    def __init_from_file(self, filename):
        file_lines = []
        comments = 0
        fd = open(filename, "r")
        for line in fd:
            if line.startswith("--"): # Skip comments
                continue
            elif line.startswith("/*"):
                comments += 1
            count = line.count("*/")
            if count > 0:
                comments -= count
                assert comments >= 0
                continue
            if comments > 0:
                continue

            if line.startswith("<"):
                match = re.compile(r"<(\S+)>").search(line)
                if match:
                    include_file = match.group(1)
                    include_lines = \
                        self.__init_from_file(\
                        os.path.join(os.path.dirname(filename),
                                     include_file))
                    file_lines.extend(include_lines)
                continue

            if line.strip():
                file_lines.append(line.strip())
        fd.close()
        return file_lines

    def get_statements(self):
        return self.__lines

    def preprocess(self, lines):
        substitutes, lines = self.scan_for_subs(lines)
        return self.apply_subs(substitutes, lines)

    def scan_for_subs(self, lines):
        outlines = []
        subs = {}
        for line in lines:
            if line.startswith('{'):
                match = re.compile(r'{(@\S+)\s*=\s*"(.*)"}$').search(line)
                subs[match.group(1).strip()] = match.group(2).strip()
            else:
                outlines.append(line)
        return subs, outlines

    def apply_subs(self, subs, lines):
        outlines = []
        for line in lines:
            for key in subs.keys():
                if line.find(key) > -1:
                    line = line.replace(key, subs[key])
            outlines.append(line)
        return outlines

class SQLGenerator:
    def __init__(self, catalog, template, is_volt):
        global IS_VOLT
        IS_VOLT = is_volt

        # Reset the counters
        IDValueGenerator(0)

        self.__operators = (CmpGenerator, MathGenerator, LogicGenerator,
                            NegationGenerator, DistinctGenerator,
                            SortOrderGenerator, AggregationGenerator,
                            SetGenerator)
        self.__variables = (TableGenerator, VarGenerator, ValueGenerator)

        if isinstance(catalog, Schema):
            self.__schema = catalog
        else:
            self.__schema = Schema(filename=catalog)

        if isinstance(template, Template):
            self.__template = template
        else:
            self.__template = Template(filename=template)

        self.__statements = self.__template.get_statements()

    def __generate_statement(self, statement):
        tmp = statement.next()
        if tmp == None:
            for stmt in  statement.get_statement():
                yield stmt
        else:
            # Handle Variables.
            if isinstance(tmp[0], VariableGenerator):
                if isinstance(tmp[1][1][0], TableGenerator):
                    if not statement.get_tables():
                        params_perm = (self.__schema.get_tables(),)
                    else:
                        params_perm = (statement.get_tables(),)
                elif isinstance(tmp[1][1][0], VarGenerator):
                    params_perm = \
                        self.__schema.pick_columns(statement.get_table_count(),
                                                   1,
                                                   statement.get_tables(),
                                                   map_type(tmp[0].get_type()))
                elif isinstance(tmp[1][1][0], ValueGenerator):
                    params_perm = (None,)

                for params in params_perm:
                    for i in tmp[0].generate(tmp[1][0], params):
                        stmt = Statement(i, self.__operators, self.__variables,
                                         statement)
                        for s in self.__generate_statement(stmt):
                            yield s
                return

            c = [True for i in tmp[1][1]
                 if isinstance(i, VarGenerator)].count(True)

            if c == 0:
                for i in tmp[0].generate(tmp[1][0], (tmp[1][1],)):
                    stmt = Statement(i, self.__operators, self.__variables,
                                     statement)
                    for s in self.__generate_statement(stmt):
                        yield s
            else:
                type = map_type(tmp[1][1][0] and tmp[1][1][0].get_type() or None)
                col_gen = self.__schema.pick_columns(statement.get_table_count(),
                                                     c, statement.get_tables(),
                                                     type)
                for columns in col_gen:
                    for i in xrange(len(tmp[1][1])):
                        if tmp[1][1][i]:
                            tmp[1][1][i] = columns.pop(0)

                    for i in tmp[0].generate(tmp[1][0], (tmp[1][1],)):
                        stmt = Statement(i, self.__operators, self.__variables,
                                         statement)
                        for s in self.__generate_statement(stmt):
                            yield s

    def generate(self):
        for s in self.__statements:
            stmt = Statement(s, self.__operators, self.__variables)
            results = 0
            for i in self.__generate_statement(stmt):
                results += 1
                yield i
            if results == 0:
                print 'Template "%s" failed to yield SQL statements' % s
