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
ALLOW_SELF_JOIN = True

def field_name_generator():
    i = 0
    while True:
        yield "{field_%d}" % (i)
        i += 1
fn_generator = field_name_generator()

class IntValueGenerator:
    """This is the base generator for integers.
    """

    def __init__(self):
        self.__min = -maxint - 1
        self.__max = maxint
        self.__nullpct = 0

    def set_min_max(self, min, max):
        self.__min = min
        self.__max = max

    def set_nullpct(self, nullpct):
        self.__nullpct = nullpct

    def generate_values(self, count):
        for i in xrange(count):
            if self.__nullpct and (random.randint(0, 100) < self.__nullpct):
                yield None
            else:
                yield random.randint(self.__min, self.__max)


class ByteValueGenerator(IntValueGenerator):
    """This generates bytes.
    """

    def __init__(self):
        IntValueGenerator.__init__(self)
        self.set_min_max(-127, 127)

class Int16ValueGenerator(IntValueGenerator):
    """This generates 16-bit integers.
    """

    def __init__(self):
        IntValueGenerator.__init__(self)
        self.set_min_max(-32767, 32767)

class Int32ValueGenerator(IntValueGenerator):
    """This generates 32-bit integers.
    """

    def __init__(self):
        IntValueGenerator.__init__(self)
        self.set_min_max(-2147483647, 2147483647)

class Int64ValueGenerator(IntValueGenerator):
    """This generates 64-bit integers.
    """

    def __init__(self):
        IntValueGenerator.__init__(self)
        self.set_min_max(-9223372036854775807, 9223372036854775807)

class FloatValueGenerator:
    """This generates 64-bit float.
    """

    def __init__(self):
        self.__nullpct = 0

    def set_nullpct(self, nullpct):
        self.__nullpct = nullpct

    def generate_values(self, count):
        for i in xrange(count):
            if self.__nullpct and (random.randint(0, 100) < self.__nullpct):
                yield None
            else:
                yield random.random()


class DecimalValueGenerator:
    """This generates Decimal values.
    """

    def __init__(self):
        # currently VoltDB values support 12 digits of precision.
        # generate constant values to 3 digits of precision to give HSQL room
        # to do exact math (multiplications) within 12 bits of precision.
        # Otherwise, it complains rather than rounding.
        decimal.getcontext().prec = 3
        self.__nullpct = 0

    def set_nullpct(self, nullpct):
        self.__nullpct = nullpct

    def generate_values(self, count):
        for i in xrange(count):
            # we support 7 digits of scale, so magnify those tiny floats
            if self.__nullpct and (random.randint(0, 100) < self.__nullpct):
                yield None
            else:
                yield decimal.Decimal(str(random.random() * 100.00))


class StringValueGenerator:
    """This generates strings.
    """

    ALPHABET = u"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    def __init__(self):
        self.__nullpct = 0

    def set_nullpct(self, nullpct):
        self.__nullpct = nullpct

    def generate_values(self, count, length = 14):
        for i in xrange(count):
            list = [random.choice(StringValueGenerator.ALPHABET) for y in xrange(length)]
            if self.__nullpct and (random.randint(0, 100) < self.__nullpct):
                yield None
            else:
                yield u"".join(list)


class VarbinaryValueGenerator:
    """This generates byte strings expressed as pairs of hex digits.
    """

    HEXDIGIT = u"0123456789ABCDEF"

    def __init__(self):
        self.__nullpct = 0

    def set_nullpct(self, nullpct):
        self.__nullpct = nullpct

    def generate_values(self, count, length = 17):
        for i in xrange(count):
            list = [random.choice(VarbinaryValueGenerator.HEXDIGIT) for y in xrange(length*2)] # length*2 hex digits gives whole bytes
            if self.__nullpct and (random.randint(0, 100) < self.__nullpct):
                yield None
            else:
                yield u"".join(list)


class TimestampValueGenerator:
    """This generates timestamps in a reasonable range.
    """

    #The MIN_MILLIS_SINCE_EPOCH is the lower bound of the generator, and its timestamp is
    #1843-03-31 11:57:18.000000. The MAX_MILLIS_SINCE_EPOCH is the upper bound of the generator,
    #and its timestamp is 2027-01-15 03:00:00.000000. Negative number is to generate timestamp
    #prior to the unix epoch.
    MIN_MILLIS_SINCE_EPOCH = -3000000000
    MAX_MILLIS_SINCE_EPOCH = 1800000000

    def __init__(self):
        self.__nullpct = 0

    def set_nullpct(self, nullpct):
        self.__nullpct = nullpct

    def generate_values(self, count):
        for i in xrange(count):
            if self.__nullpct and (random.randint(0, 100) < self.__nullpct):
                yield None
            else:
                r = random.uniform(TimestampValueGenerator.MIN_MILLIS_SINCE_EPOCH, TimestampValueGenerator.MAX_MILLIS_SINCE_EPOCH)
                ts = datetime.datetime.fromtimestamp(r)
                #The format is YYYY-MM-DD HH:MM:SS.mmmmmm
                s = ts.isoformat(' ')
                #According to the python document, the datetime.isoformat() will not show
                #microsecond "mmmmmm" if datetime.microsecond is 0. So here we manually add
                #trailing zeros if datetime.microsecond is 0.
                #(https://docs.python.org/2/library/datetime.html)
                if ts.microsecond == 0:
                    s += '.000000'
                #HSQL's resolution is millisecond while VoltDB's is microsecond. We rounded
                #the timestamp down to millisecond so that both databases store the same data.
                s = s[:-3]+'000'
                yield s


class BaseGenerator:
    """This is the base class for all non-value generators (operator generator, column generator, etc.).
    """

    def __init__(self, token):
        global fn_generator

        self.__token = token
        self.fn_gen = fn_generator
        self.__fn = None
        self.__label = None
        self.values = []
        self.reserved_value = None
        self.prior_generator = None

    # For now, all generators use the same pattern to capture generator attributes,
    # even though most of them end up ignoring the attributes that don't affect them.
    # Some attributes are very general, like the label, while others like min and max apply very narrowly
    # (currently to numeric value generators).
    # The named attribute facility allows arbitrary named attributes to be specified as
    # "<name1=value1 name2=value2>" with no embedded spaces allowed except to separate attributes (as shown).
    # Generators are free to honor or ignore any such attributes.
    # For labeled tokens, like "_X[#Y...]", attributes are only processed on the first occurrence of each unique
    # token/label combination in a statement and ignored on other occurrences.
    #
    #                   token (starting with '_')
    #                   |       optional attribute section between []s
    #                   |       |
    LABEL_PATTERN_GROUP =                    "label" # optional label for variables
    #                   |       |             |
    TYPE_PATTERN_GROUP  =                                           "type" # optional type for columns, values
    #                   |       |             |                      |
    __EXPR_TEMPLATE = r"%s" r"(\[\s*" r"(#(?P<label>\w+)\s*)?" r"(?P<type>\w+)?\s*" \
                      r"(:(?P<min>(-?\d*)),(?P<max>(-?\d*)))?\s*" r"(null(?P<nullpct>(\d*)))?" r"\])?"
    #                         |                |                             |                   |
    #                         |                |                             |       end of [] attribute section
    NULL_PCT_PATTERN_GROUP  =                                               "nullpct" # optional null percentage
    #                         |                |
    MAX_VALUE_PATTERN_GROUP =                 "max" # optional max (only for numeric values)
    #                         |
    MIN_VALUE_PATTERN_GROUP ="min" # optional min (only for numeric values)

    # A simpler pattern with no group capture is used to find recurrences of (references to) definition
    # patterns elsewhere in the statement, identified by label.
    # These can either be token-type-specific, like "_variable[#number_col]" or generic equivalents
    # like "__[#number_col]" either of which would match a prior "_variable[#number_col int]".
    # Since the "__" syntax never introduces a definition, it is convenient for use as a forward
    # reference to a definition provided later on in the statement.
    #                          token (starting with '_') or just '__' to match/reuse ANY token
    #                          |     '[' required
    #                          |      |    matching label, required
    #                          |      |    |  attributes or other garbage optional/ignored
    #                          |      |    |  |     final ']' required
    #                          |      |    |  |     |
    __RECURRENCE_TEMPLATE = r"(%s|__)\[\s*#%s[^\]]*\]"

    @classmethod
    def _expr_builder(cls, tag):
        return re.compile(cls.__EXPR_TEMPLATE % (tag))

    @classmethod
    def _recurrence_builder(cls, tag, label):
        ### print "DEBUG: recurrence template: " + (cls.__RECURRENCE_TEMPLATE % (tag, label))
        return re.compile(cls.__RECURRENCE_TEMPLATE % (tag, label))

    def generate_statements(self, statement):
        """statement is an sql statement pattern which still needs some field name resolution.
           globally substitute each of the generator's candidate parameters.
        """
        for i in self.next_param():
            yield statement.replace(self.__fn, i)

    @classmethod
    def prepare_generators(cls, statement, schema, generator_types):
        """prepare fields and generators for each generator pattern in statement.
        """
        new_generators = []
        field_map = {}
        # no table generator yet
        prior_generators = {}
        for ctor in generator_types:
            while True:
                another_gen = ctor()
                rewrite, field_name = another_gen.prepare_fields(statement)
                if rewrite:
                    prior_generators = another_gen.configure_from_schema(schema, prior_generators)
                    field_map[field_name] = another_gen
                    ### print "DEBUG field_map[" + field_name + "] got " + another_gen.debug_gen_to_string()
                    new_generators.append(another_gen)
                    statement = rewrite
                else:
                    break
        return statement, new_generators, field_map

    def configure_from_schema(self, schema, unchanged_prior_generators):
        """ The generator class (unlike ColumnGenerator/TableGenerator) may not be affected by schema
        """
        return unchanged_prior_generators


    @classmethod
    def generate_statements_from_list(cls, stmt, generators, field_map):
        """A utility that generates multiple statement strings by substituting a set of values for each
           specially marked field in the input string, resulting in all the possible combinations.
           Each generator is responsible for recognizing its unique field mark and providing its set of
           substitutions.
        """
        ###TODO: Use the currently ignored field_map or build a field-to-value map dynamically to
        ### divorce value combinatorics from statement re-write.
        if generators:
            # apply the next generator
            for generated_stmt in generators[0].generate_statements(stmt):
                # apply the remaining generators
                for complete_statement in BaseGenerator.generate_statements_from_list(generated_stmt,
                                                                                      generators[1:],
                                                                                      field_map):
                    yield complete_statement
        else:
            yield stmt # saw the last generator, statement should be complete


    def prepare_fields(self, statement):
        """replace with a unique field name a definition with the generator's token and any
           (like-labeled) occurrences.
           Call prepare_params to initialize the generator with parameters from its definition.
           Return the modified statement, or None if there is no matching token in the statement.
        """

        # match the token and capture all of its attributes
        definition = self._expr_builder(self.__token);
        match = definition.search(statement)

        # Stop when statement does not have any (more) of these tokens.
        if not match:
            return None, None

        # Process the label, the one universally applicable attribute
        self.__label = match.group(BaseGenerator.LABEL_PATTERN_GROUP)

        ### print "DEBUG: prepare_fields found " + self.__token + "[#" + ( self.__label or "" ) + "]" + " IN " + statement
        # Replace the definition with a generated unique field name
        self.__fn = self.fn_gen.next()
        rewrite = definition.sub(self.__fn, statement, 1)

        # Dispatch to let the specific generator class deal with any custom attributes
        self.prepare_params(match.groupdict())

        # Anything with a label can recur, replace recurrences with the same field name.
        if self.__label:
            recurrence = self._recurrence_builder(self.__token, self.__label)
            rewrite = recurrence.sub(self.__fn, rewrite, 0)
        ### print "DEBUG: prepare_fields after  " + self.__token + "[#" + ( self.__label or "" ) + "]" + " IN " + rewrite
        return rewrite, self.__fn

    def prepare_params(self, attribute_groups):
        """ abstract method implemented by all derived classes """
        pass

    def next_param(self):
        for value in self.values:
            if self.prior_generator:
                if self.prior_generator.has_reserved(value):
                    continue # To avoid self-join and other kinds of redundancy, don't reuse values.
            self.reserved_value = value
            yield value

    def has_reserved(self, name):
        if name == self.reserved_value:
            return True
        if not self.prior_generator:
            return False
        return self.prior_generator.has_reserved(name)

    def debug_gen_to_string(self):
        result = "generator: " + self.__token + " VALUES: "
        for val in self.values:
            result += val + ", "
        if self.reserved_value:
            result += "reserved: " + self.reserved_value
        return result


class TableGenerator(BaseGenerator):
    """This replaces occurrences of token "_table" with a schema table name.
       For each statement, each of the tables from the current schema are bound to one of
       these _table generators in sequence to purposely avoid accidental repeats (self-joins).
       Occurrences of the same table name within a statement should be worked around via SQL aliases.
    """

    def __init__(self):
        BaseGenerator.__init__(self, "_table")

    def configure_from_schema(self, schema, prior_generators):
        self.values = schema.get_tables()
        self.prior_generator = prior_generators.get("table")
        prior_generators["table"] = self # new table generator at the head of the chain
        return prior_generators

    def has_reserved(self, name):
        if ALLOW_SELF_JOIN:
            return False
        return super().has_reserved(name)


class ColumnGenerator(BaseGenerator):
    """This replaces occurrences of token _variable with a column name.
       Within a statement, intended occurrences of the same column name must use the same '#label'.
       Attributes only matter on the first occurence of "_variable" for a given label.
       As a convenience, forward references can use the __[#label] syntax instead of _variable[#label]
       to defer locking in attribute settings until a later _variable occurrence.

       The column name is selected from the schema columns of any/all tables in the schema.
       As a result, inclusion of tables that define different column names in a single schema can
       result in test runs that mostly test error cases that reference undefined columns on
       particular tables.
    """

    def __init__(self):
        BaseGenerator.__init__(self, "_variable")

    def prepare_params(self, attribute_groups):
        self.__supertype = attribute_groups[BaseGenerator.TYPE_PATTERN_GROUP]
        if not self.__supertype:
            self.__supertype = ""

    def configure_from_schema(self, schema, prior_generators):
        """ Get matcing column values fom schema
        """
        self.values = schema.get_typed_columns(self.__supertype)
        self.prior_generator = prior_generators.get("variable")
        prior_generators["variable"] = self # new variable generator at the head of the chain
        return prior_generators


class ConstantGenerator(BaseGenerator):
    """This replaces a variable with an actual constant value.
    """

    TYPES = {"int": IntValueGenerator,
             "byte": ByteValueGenerator,
             "int16": Int16ValueGenerator,
             "int32": Int32ValueGenerator,
             "int64": Int64ValueGenerator,
             "float": FloatValueGenerator,
             "string": StringValueGenerator,
             "varbinary": VarbinaryValueGenerator,
             "decimal": DecimalValueGenerator,
             "timestamp": TimestampValueGenerator}

    def __init__(self):
        BaseGenerator.__init__(self, "_value")

        self.__type = None

    def prepare_params(self, attribute_groups):
        self.__type = attribute_groups[BaseGenerator.TYPE_PATTERN_GROUP]
        if not self.__type:
            print "Generator parse error -- invalid type"
            assert self.__type

        min = attribute_groups[BaseGenerator.MIN_VALUE_PATTERN_GROUP]
        max = attribute_groups[BaseGenerator.MAX_VALUE_PATTERN_GROUP]

        self.__value_generator = ConstantGenerator.TYPES[self.__type]()

        if min != None and max != None:
            self.__value_generator.set_min_max(int(min), int(max))

        nullpct = attribute_groups[BaseGenerator.NULL_PCT_PATTERN_GROUP]
        if nullpct:
            self.__value_generator.set_nullpct(int(nullpct))


    def next_param(self):
        for i in self.__value_generator.generate_values(COUNT):
            if i == None:
                i = u"NULL"
            elif isinstance(i, basestring):
                i = u"'%s'" % (i)
            elif isinstance(i, float):
                i = u"%.20e" % (i)
            yield unicode(i)


class IdGenerator(BaseGenerator):
    """This replaces _id with a counter value unique to the entire run
       (at least unless/until reset with the 'initialize' class method).
    """

    counter = 1

    def __init__(self):
        BaseGenerator.__init__(self, "_id")

    def prepare_params(self, attribute_groups):
        pass

    def next_param(self):
        id = self.__class__.counter
        self.__class__.counter += 1
        yield unicode(id)

    @classmethod
    def initialize(cls, start):
        cls.counter = start

class LiteralGenerator:
    """This generates a piece of literal query text,
       usually as one of multiple choices for a MacroGenerator
    """

    def __init__(self, literal):
        self.__literal = literal

    def generate_text(self):
        yield self.__literal


class MacroGenerator:
    """This generates pieces of literal text chosen non-randomly in turn from a list of
       LiteralGenerator snippets that were added to the macro generator using the generator
       macro building syntax:
       {@name |= "one option"}
       {@name |= "another option"}
    """

    def __init__(self):
        self.__choices = []

    def add_choice(self, generator_list):
        self.__choices.append(generator_list)

    def generate_text(self):
        for generator_list in self.__choices:
            for statement in self.generate_text_from_list(generator_list):
                yield statement

    @classmethod
    def generate_text_from_list(cls, generator_list):
        if not generator_list:
            yield ""
        else:
            for fragment_head in generator_list[0].generate_text():
                for fragment_tail in cls.generate_text_from_list(generator_list[1:]):
                    yield fragment_head + fragment_tail


class Schema:
    SUPERTYPES = {
        "byte":      ("byte",  "int", "numeric", ""),
        "int16":     ("int16", "int", "numeric", ""),
        "int32":     ("int32", "int", "numeric", ""),
        "int64":     ("int64", "int", "numeric", ""),
        "float":     ("float",        "numeric", ""),
        "decimal":   ("decimal",      "numeric", ""),
        "string":    ("string",    "nonnumeric", ""),
        "timestamp": ("timestamp", "nonnumeric", ""),
    }

    TYPE_NAMES = {
        FastSerializer.VOLTTYPE_TINYINT:   "byte",
        FastSerializer.VOLTTYPE_SMALLINT:  "int16",
        FastSerializer.VOLTTYPE_INTEGER:   "int32",
        FastSerializer.VOLTTYPE_BIGINT:    "int64",
        FastSerializer.VOLTTYPE_FLOAT:     "float",
        FastSerializer.VOLTTYPE_STRING:    "string",
        FastSerializer.VOLTTYPE_DECIMAL:   "decimal",
        FastSerializer.VOLTTYPE_TIMESTAMP: "timestamp",
    }

    def __init__(self, **kwargs):
        if 'filename' in kwargs:
            self.__init_from_file(kwargs['filename'])
        elif 'schema' in kwargs:
            self.__schema = kwargs['schema']
        else:
            print "No schema provided"
        self.__col_by_type = {}
        self.__col_by_type[""] = {}
        self.__col_by_type["int"] = {}
        self.__col_by_type["numeric"] = {}
        self.__col_by_type["nonnumeric"] = {}
        for code, supertype in Schema.TYPE_NAMES.iteritems():
            self.__col_by_type[supertype] = {}

        for table, tabledict in self.__schema.iteritems():
            for column in tabledict["columns"]:
                column_name = column[0];
                type_name = Schema.TYPE_NAMES[column[1]]
                for supertype in Schema.SUPERTYPES[type_name]:
                    # The column_name "keys" inserted here are the real data
                    # -- the set of unique column names which by convention are usually
                    # defined and typed identically on all of the tables in the schema.
                    # The table value is just documentation for the curious.
                    # It represents the last table that defined the column as
                    # listed in the schema, so it's usually just the last table in the schema.
                    self.__col_by_type[supertype][column_name] = table

    def __init_from_file(self, filename):
        fd = open(filename, "r")
        self.__content = fd.read()
        fd.close()
        self.__schema = eval(self.__content.strip())

    def get_tables(self):
        return self.__schema.keys()

    def get_typed_columns(self, supertype):
        return self.__col_by_type[supertype].keys()

    def debug_schema_to_string(self):
        result = "TABLES: "
        for table in self.get_tables():
            result += table + ", "

        result += "COLUMNS: "
        for code, supertype in Schema.TYPE_NAMES.iteritems():
            for column_name in self.get_typed_columns(supertype):
                result += supertype + " " + column_name + ", "
        return result


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
        # Collect and filter out macro definitions
        self.scan_for_macros()

    LINE_COMMENT_PATTERN = re.compile(r'^\s*((--)|$)')    # whitespace lines are comments, too

    INCLUDE_PATTERN = re.compile(r'^\s*<(\S+)>')

    def __init_from_file(self, filename):
        file_lines = []
        fd = open(filename, "r")
        for line in fd:
            if Template.LINE_COMMENT_PATTERN.search(line): # Skip classic SQL-style comments
                continue
            match = Template.INCLUDE_PATTERN.search(line)
            if match:
                include_file = match.group(1)
                include_lines = \
                    self.__init_from_file(os.path.join(os.path.dirname(filename), include_file))
                file_lines.extend(include_lines)
                continue

            file_lines.append(line.strip())
        fd.close()
        return file_lines

    def get_statements(self):
        return self.__lines

    MACRO_DEFINE_PATTERN     = re.compile(r'{' r'(@\w+)' r'\s*=\s*'   r'"(.*)"' r'\s*}' r'(\s*--.*)?$')
    GENERATOR_DEFINE_PATTERN = re.compile(r'{' r'(_\w+)' r'\s*\|=\s*' r'"(.*)"' r'\s*}' r'(\s*--.*)?$')

    def scan_for_macros(self):
        lines_out = []
        self.__macros = {}
        self.__generators = {}
        for line in self.__lines:
            if line.startswith('{'):
                match = Template.MACRO_DEFINE_PATTERN.search(line)
                if match:
                    self.__macros[match.group(1)] = match.group(2)
                else:
                    #Recognize and cache values for generator ("|=") macros.
                    match = Template.GENERATOR_DEFINE_PATTERN.search(line)
                    if match:
                        generator_name = match.group(1)
                        if generator_name not in self.__generators.keys():
                            self.__generators[generator_name] = MacroGenerator()
                        option = self.apply_macros(match.group(2))
                        # Each option value gets recursively expanded here into a choice list.
                        choice_list = self.__apply_generators(option)
                        ### print "DEBUG:adding generator " + generator_name + " option " + option + " choice list size: ", len(choice_list)
                        self.__generators[generator_name].add_choice(choice_list)
                    else:
                        print "WARNING: ignoring malformed definition: (" + line + ")"
            else:
                lines_out.append(line)
        self.__lines = lines_out

    MACRO_NAME_PATTERN = re.compile(r'@\w+')

    def apply_macros(self, line):
        pos = 0
        while True:
            # Check for something to expand
            match = Template.MACRO_NAME_PATTERN.search(line[pos:])
            if not match:
                ### print 'VERBOSE DEBUG no more macros for line "' + line + '"'
                return line
            key = match.group()
            # This could be a false positive. Check for exact key match
            sub = self.__macros.get(key, None)
            if sub == None:
                # nothing to see here. move along.
                pos += len(key)
                ### print 'VERBOSE DEBUG no macro defined for key "' + key + '"'
                continue
            pos += match.start()
            ### print 'VERBOSE DEBUG key "' + key + '" becomes "' + sub + '"'
            line = line[0:pos] + sub + line[pos+len(key):]

    GENERATOR_NAME_PATTERN = re.compile(r'_\w+')

    def __apply_generators(self, line):
        pos = 0
        while True:
            # Check for something to expand
            match = Template.GENERATOR_NAME_PATTERN.search(line[pos:])
            if not match:
                # The entire line represents a "choice" of one literal option.
                return [LiteralGenerator(line)]

            key = match.group()
            pos += match.start()
            # This could be a false positive. Check for exact key match
            choice = self.__generators.get(key, None)
            if choice:
                break
            # false alarm. nothing to see here. move along.
            pos += len(key)

        prefix = line[0:pos]
        # The prefix only has one fixed literal option.
        if prefix:
            result = [LiteralGenerator(prefix)]
        else:
            result = []
        # Add the clause list or table as the next listed choice.
        result.append(choice)

        # Since options don't contain recursive unresolved references to generators
        # only the tail needs to be recursively processed and its resulting choice list
        # tacked on to the result.
        return result + self.__apply_generators(line[pos+len(key):])


    def generate_statements_from_text(self, text):
        generator_list = self.__apply_generators(text)
        for statement in MacroGenerator.generate_text_from_list(generator_list):
            yield statement


class SQLGenerator:
    def __init__(self, catalog, template, subversion_generation):
        self.__subversion_generation = subversion_generation
        # Reset the counters
        IdGenerator.initialize(0)

        if isinstance(catalog, Schema):
            self.__schema = catalog
        else:
            self.__schema = Schema(filename=catalog)

        if isinstance(template, Template):
            self.__template = template
        else:
            self.__template = Template(filename=template)

        self.__statements = self.__template.get_statements()


    GENERATOR_TYPES = (TableGenerator, ColumnGenerator, ConstantGenerator, IdGenerator)

    UNRESOLVED_PUNCTUATION = re.compile(r'[][#@]') # Literally, ']', '[', '#', or '@'.
    UNRESOLVED_GENERATOR = re.compile(r'(^|\W)[_]')
    # The presence of an underbar apparently inside a quoted string after the LIKE keyword
    # is likely enough to be a false positive for an unresolved generator that it is
    # allowed to pass without the usual warning triggered by a leading underbar.
    LIKELY_FALSE_ALARMS = re.compile(r"LIKE '[^']*_.*'")

    def __generate_statement(self, text):
        text = self.__template.apply_macros(text)
        text = unicode(text)

        for statement in self.__template.generate_statements_from_text(text):
            ### print ('VERBOSE DEBUG: text and statement post-generate_statements_from_text: "' + text + '", "' + statement + '"')
            statement, generators, field_map = BaseGenerator.prepare_generators(statement,
                                                                     self.__schema,
                                                                     SQLGenerator.GENERATOR_TYPES)
            ### print ('VERBOSE DEBUG: prepared statement looks like: "' + statement + '"')
            if (SQLGenerator.UNRESOLVED_PUNCTUATION.search(statement) or
                (SQLGenerator.UNRESOLVED_GENERATOR.search(statement) and
                 not SQLGenerator.LIKELY_FALSE_ALARMS.search(statement))):
                print ('WARNING: final statement contains suspicious unresolved symbol(s): "' +
                       statement + '"')
                print ('with schema "' + self.__schema.debug_schema_to_string() + '"')
            for generated_stmt in BaseGenerator.generate_statements_from_list(statement,
                                                                              generators,
                                                                              field_map):
                yield generated_stmt

    def generate(self, summarize_successes = False):
        for s in self.__statements:
            results = 0
            ### print 'DEBUG VERBOSELY SPOUTING INPUT STATEMENT: ' + s
            for i in self.__generate_statement(s):
                results += 1
                ### print 'DEBUG VERBOSELY SPOUTING OUTPUT STATEMENT: ' + i
                yield i
                ### TODO: make generation of the subquery wrapping variant of the select statements optional by some global flag
                if self.__subversion_generation and re.match("(?i)\s*SELECT", i):
                    results += 1
                    yield 'SELECT * FROM (' + i + ') subquery'

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
    generator = SQLGenerator(catalog, template, True, False)
    for i in generator.generate(True):
        print 'STATEMENT: ' + i
