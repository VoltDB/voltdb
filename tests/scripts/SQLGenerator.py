#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2012 VoltDB Inc.
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
from voltdbclient import * # for VoltDB types
from optparse import OptionParser # for use in standalone test mode

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
        yield "{field_%d}" % (i)
        i += 1
fn_generator = field_name_generator()

class BaseValueGenerator:
    """This is the base class for all value generators (int, float, etc.)
    """

    def __init__(self):
        pass

    def generate(self, count):
        pass


class IntValueGenerator(BaseValueGenerator):
    """This is the base generator for integers.
    """

    def __init__(self, min = (-maxint - 1), max = maxint):
        BaseValueGenerator.__init__(self)

        self.__min = min
        self.__max = max

    def generate(self, count):
        for i in xrange(count):
            yield random.randint(self.__min, self.__max)



class IDValueGenerator(BaseValueGenerator):
    """This generates unique incremental integers.
    """

    counter = 0

    def __init__(self):
        BaseValueGenerator.__init__(self)

    @classmethod
    def initialize(cls, start):
        cls.counter = start

    def generate(self, count):
        for i in xrange(count):
            id = self.__class__.counter
            self.__class__.counter += 1
            yield id

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
        for i in xrange(count):
            yield random.random()


class DecimalValueGenerator(BaseValueGenerator):
    """This generates Decimal values.
    """

    def __init__(self):
        BaseValueGenerator.__init__(self)
        # currently we only support 12 digits of precision
        decimal.getcontext().prec = 12

    def generate(self, count):
        for i in xrange(count):
            # we support 7 digits of scale, so magnify those tiny floats
            yield decimal.Decimal(str(random.random() * 1e2))


class StringValueGenerator(BaseValueGenerator):
    """This generates strings.
    """

    ALPHABET = u"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    def __init__(self):
        BaseValueGenerator.__init__(self)

    def generate(self, count, length = 17):
        for i in xrange(count):
            list = [random.choice(self.ALPHABET) for y in xrange(length)]
            yield u"".join(list)

class VarbinaryValueGenerator(BaseValueGenerator):
    """This generates byte strings expressed as pairs of hex digits.
    """

    HEXDIGIT = u"0123456789ABCDEF"

    def __init__(self):
        BaseValueGenerator.__init__(self)

    def generate(self, count, length = 17):
        for i in xrange(count):
            list = [random.choice(self.HEXDIGIT) for y in xrange(length*2)] # length*2 hex digits gives whole bytes
            yield u"".join(list)

class DateValueGenerator(BaseValueGenerator):
    """This generates dates.
    """

    MAX_DT = 9999999999999

    def __init__(self):
        BaseValueGenerator.__init__(self)

    def generate(self, count):
        for i in xrange(count):
            # HSQL doesn't support microsecond, generate a 13 digit number of milliseconds
            # this gets scaled to microseconds later for VoltDB backends
            yield random.randint(0, self.MAX_DT)


class BaseGenerator:
    """This is the base class for all non-value generators (operator generator, column generator, etc.).
    """

    def __init__(self, token):
        global fn_generator

        self.__token = token
        self.fn_gen = fn_generator
        self.__fn = None
        self.__label = None

    # For now, all generators use the same pattern to capture generator attributes,
    # even though most of them end up ignoring the attributes that don't affect them.
    # Some attributes are very general, like the label, while others like min and max apply very narrowly
    # (currently to numeric value generators).
    # The named attribute facility allows arbitrary named attributes to be specified as
    # "<name1=value1 name2=value2>" with no embedded spaces allowed except to separate attributes (as shown).
    # Generators are free to honor or ignore any such attributes.
    # For labeled tokens, like "_X[@Y...]", attributes are only processed on the first occurrence of each unique
    # token/label combination in a statement and ignored on other occurrences.
    #
    #                   --- token (starting with '_')
    #                   |       --- optional attribute section between []s
    #                   |       |
    LABEL_PATTERN_GROUP =                    "label" # optional label
    #                   |       |             |
    KEY_VALUE_PATTERN_GROUP     =                                        "key_values" # optional named attributes (only for columns)
    #                   |       |             |                           |
    #                   |       |             |                           |
    __EXPR_TEMPLATE = r"%s" r"(\[\s*" r"(@(?P<label>\w+)\s*)?" r"(<\s*(?P<key_values>(\w+=[^>\s]+\s*)*)>)?\s*" \
                      r"(?P<type>\w+)?\s*" r"(:(?P<min>(\d*)),(?P<max>(\d*)))?" r"\])?"
    #                       |                      |              |                |
    #                       |                      |              |                --- end of [] attribute section
    MAX_VALUE_PATTERN_GROUP =                                    "max" # optional max (only for numeric values)
    #                       |                      |
    MIN_VALUE_PATTERN_GROUP =                     "min" # optional min (only for numeric values)
    #                       |
    TYPE_PATTERN_GROUP =   "type" # optional type (for columns, values, maybe someday aggs/ops/functions?)
    # The type attribute could arguably be dropped in favor of recognizing "< type=X >" within the key-value attributes.

    # A simpler pattern with no group capture is used to find recurrences of (references to) definition patterns elsewhere in the statement,
    # identified by label.
    # These can either be token-type-specific, like "_variable[@number_col]" or generic equivalents "__[@number_col]" either of which would
    # match a prior "_variable[@number_col int]".
    # Since the "__" syntax does not introduce a definition, it can also be used as a forward reference, as convenient.
    #                          .-- token (starting with '_') or just '__' to match/reuse ANY token
    #                          |      .-- '[' required
    #                          |      |    .-- matching label, required
    #                          |      |    |  .-- attributes or other garbage optional/ignored
    #                          |      |    |  |    .-- final ']' required
    #                          |      |    |  |    |
    __RECURRENCE_TEMPLATE = r"(%s|__)\[\s*@%s[^\]]*\]"

    @classmethod
    def _expr_builder(cls, tag):
        return re.compile(cls.__EXPR_TEMPLATE % (tag))

    @classmethod
    def _recurrence_builder(cls, tag, label):
        ### print "DEBUG: recurrence template: " + (cls.__RECURRENCE_TEMPLATE % (tag, label))
        return re.compile(cls.__RECURRENCE_TEMPLATE % (tag, label))

    def generate(self, statement):
        """statement is an sql statement pattern which still needs some field name resolution.
           globally substitute each of the generator's candidate parameters.
        """
        for i in self.next_param():
            yield statement.replace(self.__fn, i)

    def prepare_operands(self, statement):
        """mark with a unique field name a definition with the generator's token and any (like-labeled) occurrences
        """

        # match the token and capture all of its attributes
        definition = self._expr_builder(self.__token);
        match = definition.search(statement)

        # Stop when statement does not have any (more) of these tokens.
        if not match:
            return None

        # Process the label, the one universally applicable attribute
        self.__label = match.group(self.LABEL_PATTERN_GROUP)

        ### print "DEBUG: prepare_operands found " + self.__token + "[@" + ( self.__label or "" ) + "]" + " IN " + statement
        # Replace the definition with a generated unique field name
        self.__fn = self.fn_gen.next()
        stmt = definition.sub(self.__fn, statement, 1)

        # Dispatch to let the specific generator class deal with any custom attributes
        self.prepare_params(match.groupdict())

        # Anything with a label can recur, replace recurrences with the same field name.
        if self.__label:
            recurrence = self._recurrence_builder(self.__token, self.__label)
            stmt = recurrence.sub(self.__fn, stmt, 0)
        ### print "DEBUG: prepare_operands after  " + self.__token + "[@" + ( self.__label or "" ) + "]" + " IN " + stmt
        return stmt

    def prepare_params(self, attribute_groups):
        """ abstract method implemented by all derived classes """
        pass

    def next_param(self):
        """ abstract method implemented by all derived classes """
        pass

    def get_named_attribute(self, kv, key):
        """a helper for parsing key-value pairs. Assumes no embedded spaces in key-value pairs.
        """
        # Split on white space into key-value pairs keeps the parser dirt-simple.
        pairs = kv.split();
        for pair in pairs:
            # Match the "key=" part and return the rest as the value.
            match = re.match(key + '=([^\s]*)', pair)
            if match:
                return match.group(1)


class TableGenerator(BaseGenerator):
    """This replaces occurrences of token "_table" with a schema table name.
       Within a statement, intended occurrences of the same table name must use the same '@label'.
       Tables in the current schema are bound to these _table generators for a statement
       in a particular pattern that purposely avoids accidental repeats (self-joins) and
       falls way short of "all combinations" or even "all permutations".
       -- see class SQLGenerator.
    """

    def __init__(self):
        BaseGenerator.__init__(self, "_table")
        self.__tables = []
        self.__table = None

    def add_table(self, table):
        self.__tables.append(table)

    def get_table(self):
        return self.__table

    def next_param(self):
        for i in self.__tables:
            self.__table = i
            yield i


class ColumnGenerator(BaseGenerator):
    """This replaces occurrences of token _variable with a column name.
       Within a statement, intended occurrences of the same column name must use the same '@label'.
       Attributes only matter on the first occurence of "_variable"
       for a given label.
       As a convenience, forward references can use the __[@label] syntax instead of _variable[@label]
       to defer locking in attribute settings to the later _variable occurrence.

       By default, the column name is selected from the schema columns of whichever table
       is currently bound to the first _table occurence in the statement. This can be
       overridden using the <table=tablename> syntax in the column's attributes (placed after the column's
       optional @label and before any unnamed column attributes, e.g. its type), e.g.
           _variable[@filtered_by <table=MyTestTable> int] > 0
       which would generate statements that filtered on each of MyTestTable's int-typed columns, or
           _variable[int] = _variable[<table=_table[@rhs_table]> int]
       which would generate a join filter pairing each integer column from the first _table (implied)
       with each integer column from some other table bound to @rhs_label.
    """

    def __init__(self, default_table):
        BaseGenerator.__init__(self, "_variable")
        self.__default_table = default_table

    def prepare_params(self, attribute_groups):
        self.__type = attribute_groups[self.TYPE_PATTERN_GROUP]
        self.__table = None
        kv = attribute_groups[self.KEY_VALUE_PATTERN_GROUP]
        if kv:
            self.__table = self.get_named_attribute(kv, "table")
        if not self.__table:
            self.__table = self.__default_table
        self.__columns= []

    def get_table(self):
        return self.__table

    def get_type(self):
        return self.__type

    def add_column(self, col_name, col_type):
        self.__columns.append((col_name, col_type))

    def next_param(self):
        for (col_name, col_type) in self.__columns:
            self.__type = col_type ## cache for later reference by type-matching values (and functions?)
            yield col_name


class ConstantGenerator(BaseGenerator):
    """This replaces variable with actual value.
    """

    TYPES = {"id": IDValueGenerator,
             "int": IntValueGenerator,
             "byte": ByteValueGenerator,
             "int16": Int16ValueGenerator,
             "int32": Int32ValueGenerator,
             "int64": Int64ValueGenerator,
             "float": FloatValueGenerator,
             "string": StringValueGenerator,
             "varbinary": VarbinaryValueGenerator,
             "decimal": DecimalValueGenerator,
             "date": DateValueGenerator}

    def __init__(self):
        BaseGenerator.__init__(self, "_value")

        self.__type = None

    def prepare_params(self, attribute_groups):
        self.__type = attribute_groups[self.TYPE_PATTERN_GROUP]
        assert self.__type

        min = attribute_groups[self.MIN_VALUE_PATTERN_GROUP]
        max = attribute_groups[self.MAX_VALUE_PATTERN_GROUP]

        if min != None and max != None:
            self.__values = self.TYPES[self.__type](int(min), int(max))
        else:
            self.__values = self.TYPES[self.__type]()

    def next_param(self):
        for i in self.__values.generate(COUNT):
            if IS_VOLT and self.__type == "date":
                i = i * 1000
            elif isinstance(i, basestring):
                i = u"'%s'" % (i)
            elif isinstance(i, float):
                i = u"%.20e" % (i)
            yield unicode(i)


class PickGenerator(BaseGenerator):
    """This generates statement elements picked from a specified options list, e.g.
           select _pick[<options=MIN,MAX>](_variable) from _table;
       would generate statements that tried MAX or MIN but not other aggregate functions.
       Useful for generating variants that are specialized within a classification like "_agg"
       or that defy existing classifications.
    """

    def __init__(self):
        BaseGenerator.__init__(self, "_pick")
        self.__options = [' ']

    def prepare_params(self, attribute_groups):
        kv = attribute_groups[self.KEY_VALUE_PATTERN_GROUP]
        if kv:
            self.__options = self.get_named_attribute(kv, "options")
        else:
            print "ERROR: Invalid attribute list for _pick."

    def next_param(self):
        for option in self.__options.split(','):
            if option == '_': # special notation to allow a single empty option, as in <options=_>
                yield ' '
            else:
                yield option


class OpGenerator(BaseGenerator):
    """This is the base generator for all operator generators.
    """

    def __init__(self, ops, token):
        BaseGenerator.__init__(self, token)

        self.__ops = ops

    def next_param(self):
        for i in self.__ops:
            yield i


class AggregationGenerator(OpGenerator):
    """This generates statements with aggregation functions.
    """

    def __init__(self):
        OpGenerator.__init__(self, ("SUM", "AVG", "MIN", "MAX", "COUNT"), "_agg")

class NumericAggregationGenerator(OpGenerator):
    """This generates statements with aggregation functions.
    """

    def __init__(self):
        OpGenerator.__init__(self, ("SUM", "MIN", "MAX", "COUNT"), "_genericagg")

class NegationGenerator(OpGenerator):
    """This generator generates statements using the not operator.
    """

    def __init__(self):
        OpGenerator.__init__(self, ("NOT", ""), "_singleton")

class DistinctGenerator(OpGenerator):
    """This generator generates statements optionally using the distinct operator.
    """

    def __init__(self):
        OpGenerator.__init__(self, ("DISTINCT", ""), "_distinct")

class SortOrderGenerator(OpGenerator):
    """This generator generates sort order ASC, DESC, blank for ORDER BY
    """

    def __init__(self):
        OpGenerator.__init__(self, ("", "ASC", "DESC"), "_sortorder")

class MathGenerator(OpGenerator):
    """This generator generates statements using different operators (+, -, *, /, %).
    """

    def __init__(self):
        #OpGenerator.__init__(self, ("+", "-", "*", "/", "%"), "_math")
        OpGenerator.__init__(self, ("+", "-", "*", "/"), "_math")

class CmpGenerator(OpGenerator):
    """This generator generates statements using comparison operators (<, >, =, etc.).
    """

    def __init__(self):
        OpGenerator.__init__(self, ("<", ">", "=", "<=", ">=", "!=", "<>"), "_cmp")

class LikeGenerator(OpGenerator):
    """This generator generates statements using LIKE / NOT LIKE.
    """

    def __init__(self):
        OpGenerator.__init__(self, ("LIKE", "NOT LIKE"), "_like")

class SetGenerator(OpGenerator):
    """This generates statements using set operators (union, etc.).
    """

    def __init__(self):
        OpGenerator.__init__(self, ("UNION",), "_set")

class LogicGenerator(OpGenerator):
    """This generates statements using logic operators (AND, OR).
    """

    def __init__(self):
        #OpGenerator.__init__(self, ("AND", "OR"), "_logic")
        OpGenerator.__init__(self, ("AND",), "_logic")


class Statement:
    def __init__(self, text):
        self.__text = unicode(text)
        self.__generator_types = (CmpGenerator, MathGenerator, LogicGenerator,
                                  NegationGenerator, DistinctGenerator,
                                  SortOrderGenerator, AggregationGenerator, NumericAggregationGenerator,
                                  SetGenerator, ConstantGenerator, PickGenerator)

        # prepare table generators
        self.__statement = self.__text
        self.__table_gens = []
        self.__first_table = None
        table_gen = TableGenerator()
        ret = table_gen.prepare_operands(self.__statement)

        while ret:
            self.__table_gens.append(table_gen)
            self.__statement = ret
            table_gen = TableGenerator()
            ret = table_gen.prepare_operands(self.__statement)

    def get_tables(self):
        return self.__table_gens

    def generate_statements(self, stmt, gens, depth):
        if depth < len(gens):
            # apply the next generator
            for generated_stmt in gens[depth].generate(stmt):
                # apply the remaining generators
                for complete_statement in self.generate_statements(generated_stmt, gens, depth+1):
                    yield complete_statement
        else:
            yield stmt # saw the last generator, statement should be complete

    def next_table_bound_statement(self):
        for table_bound_statement in self.generate_statements(self.__statement, self.__table_gens, 0):
            if self.__table_gens:
                self.__first_table = self.__table_gens[0].get_table()
            self.__col_gens = []
            yield table_bound_statement

    def init_column_generators(self, table_bound_statement):
        # prepare columns according to the selection of tables
        col_gen = ColumnGenerator(self.__first_table)
        ret = col_gen.prepare_operands(table_bound_statement)

        while ret:
            self.__col_gens.append(col_gen)
            table_bound_statement = ret
            col_gen = ColumnGenerator(self.__first_table)
            ret = col_gen.prepare_operands(table_bound_statement)

        self.__table_bound_statement = table_bound_statement
        return self.__col_gens

    def next_complete_statement(self):
        for column_bound_statement in self.generate_statements(self.__table_bound_statement, self.__col_gens, 0):

            # prepare operators and constants according to the selection of column (types)
            other_gens = []
            for ctor in self.__generator_types:
                other_gen = ctor()
                ret = other_gen.prepare_operands(column_bound_statement)

                while ret:
                    other_gens.append(other_gen)
                    column_bound_statement = ret
                    other_gen = ctor()
                    ret = other_gen.prepare_operands(column_bound_statement)

            for complete_statement in self.generate_statements(column_bound_statement, other_gens, 0):
                # Finally, no more tokens!
                yield complete_statement


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
        IDValueGenerator.initialize(0)

        if isinstance(catalog, Schema):
            self.__schema = catalog
        else:
            self.__schema = Schema(filename=catalog)

        if isinstance(template, Template):
            self.__template = template
        else:
            self.__template = Template(filename=template)

        self.__statements = self.__template.get_statements()

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

    def get_schema_typed_columns(self, table, type_str, quota):
        if type_str in self.TYPES:
            types = self.TYPES[type_str]
        else:
            types = None

        typed_columns = [(column[0], column[1])
                         for column in self.__schema.get_columns(table)
                         if (not types) or (column[1] in types)]
        # randomly eliminate candidates to keep under our quota
        while len(typed_columns) > quota:
            i = random.randint(0,len(typed_columns)-1)
            del typed_columns[i:i+1]
        return typed_columns

    def get_schema_tables(self):
        return self.__schema.get_tables()

    def __generate_statement(self, text):
        statement = Statement(text)
        table_gens = statement.get_tables()

        if table_gens:
            schema_tables = self.get_schema_tables()
            # This is the odd-ish algorithm for assigning schema tables to table generators.
            # Instead of trying each schema table in each role (i.e. substituted for each _table),
            # the schema tables are distributed "round robin" among them.
            # If there are more unique _tables than there are schema tables, some _table(s) will be
            # starved for a suitable substitution, so no statement will be generated.
            for i in xrange(0, len(schema_tables), len(table_gens)):
                for k, v in zip(table_gens, schema_tables[i:]):
                    k.add_table(v)

        # Start by binding the tables, which may have an effect on column choices
        # (if they have a table=_table[@label] attribute).
        for table_bound_statement in statement.next_table_bound_statement():
            col_gens = statement.init_column_generators(table_bound_statement)
            # Establish the working set of each column generator.
            for col_gen in col_gens:
                for (col_name, col_type) in self.get_schema_typed_columns(col_gen.get_table(),
                                                                          col_gen.get_type(),
                                                                          COUNT):
                    col_gen.add_column(col_name, col_type)

            # Finish by binding columns and then other things (values and operators) that might
            # (sometimes, some day? by use of labeled tokens in their attributes)
            # be influenced by column choices.
            for complete_statement in statement.next_complete_statement():
                yield complete_statement

    def generate(self, summarize_successes = False):
        for s in self.__statements:
            results = 0
            ### print 'DEBUG VERBOSELY SPOUTING INPUT STATEMENT: ' + s
            for i in self.__generate_statement(s):
                results += 1
                ### print 'DEBUG VERBOSELY SPOUTING OUTPUT STATEMENT: ' + i
                yield i
            if results == 0:
                print 'Template "%s" failed to yield SQL statements' % s
            elif summarize_successes:
                print 'Template "%s" yielded (%d) SQL statements' % (s, results)

if __name__ == "__main__":
    # run the SQLGenerator in a test mode that simply prints its results
    # given the schema file and statement file referenced on the command line.
    # The schema file should not contain any generic "@macros" only '_'-prefixed
    # generator tokens (with optional []-bracketed attributes and @-prefixed labels).
    parser = OptionParser()
    parser.add_option("-s", "--seed", dest="seed",
                      help="seed for random number generator")
    (options, args) = parser.parse_args()

    if options.seed == None:
        seed = random.randint(0, 2**63)
        print "Random seed: %d" % seed
    else:
        seed = int(options.seed)
        print "Using supplied seed: " + str(seed)
    random.seed(seed)

    if len(args) < 2:
        usage()
        sys.exit(3)

    catalog = args[0]
    template = args[1]
    generator = SQLGenerator(catalog, template, False)
    for i in generator.generate(True):
        print 'STATEMENT: ' + i

