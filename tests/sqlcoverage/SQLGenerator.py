#!/usr/bin/env python

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

import decimal
import os.path
import re
import random
import time
import datetime
from math import ceil
from sys import maxint
from voltdbclientpy2 import * # for VoltDB types
from optparse import OptionParser # for use in standalone test mode
# Need these to print non-ascii characters:
import codecs
import sys
UTF8Writer = codecs.getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)

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
       Typically, integer values would be specified as, e.g., _value[int:0,10],
       which would yield a small number of random values (two, by default)
       between 0 and 10 (inclusive). However, it is also possible to specify a
       count, e.g., _value[int:0,10;5] would yield five random values between
       0 and 10; or you can specify a step, e.g., _value[int:0,10,2] (note the
       comma, rather than semi-colon) would yield the (non-random) values
       0, 2, 4, 6, 8, 10; or, you may specify both, e.g., _value[int:6,12,3;9]
       would yield the 9 (non-random) values 6, 9, 12, 8, 11, 7, 10, 6, 9;
       notice how the values increase by 3, but cycle around via the mod (%)
       operator, always between the specified min of 6 and max of 12.
       It is also possible to specify the type of integer you want, i.e.:
       _value[byte], _value[int16], _value[int32], or _value[int64], though in
       that case you will always get random values, whose min and max values
       are defined for you.
       You may also specify a null percentage, e.g., _value[byte null25] will
       yield random byte values (between -127 and 127, inclusive), with a 25%
       chance of being null.
    """

    def __init__(self):
        self.__min = -maxint - 1
        self.__max = maxint
        self.__step = 0
        self.__count = 0
        self.__nullpct = 0

    def set_min_max(self, min, max, step=0):
        self.__min = int(min)
        self.__max = int(max)
        self.__step = int(step)
        # If step is specified by count is not, set it large enough to cover
        # the range between min and max, with the given step size
        if step and not self.__count:
            self.__count = int(ceil( (self.__max + 1.0 - self.__min) / self.__step ))

    def set_count(self, count):
        self.__count = int(count)

    def set_nullpct(self, nullpct):
        self.__nullpct = nullpct

    def generate_values(self, count):
        for i in xrange(max(count, self.__count)):
            if self.__nullpct and (random.randint(0, 100) < self.__nullpct):
                yield None
            # If the step was specified, return non-random integer values,
            # starting with the min, ending with the max, increasing by the
            # step, and cycling around via the mod (%) operator, if the count
            # requires additional values
            elif self.__step:
                yield self.__min + ((i*self.__step) % (self.__max+1 - self.__min))
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


class PointValueGenerator:
    """This generates (random) point (GEOGRAPHY_POINT) values.
    """
    # It's annoying to have random numbers with 12 digits, so we limit it to
    # a small number beyond the decimal point
    DIGITS_BEYOND_DECIMAL_POINT = 2

    def __init__(self):
        decimal.getcontext().prec = PointValueGenerator.DIGITS_BEYOND_DECIMAL_POINT
        self.__nullpct = 0
        # By default, random points can be anywhere on Earth
        self.set_min_max(-180.0, 180.0, -90.0, 90.0)

    def set_min_max(self, longmin, longmax, latmin=None, latmax=None):
        self.__longmin  = float(longmin)
        self.__longdiff = float(longmax) - self.__longmin
        if latmin is not None:
            self.__latmin = float(latmin)
        else:
            self.__latmin = self.__longmin
        if latmax is not None:
            self.__latdiff = float(latmax) - self.__latmin
        else:
            self.__latdiff = float(longmax) - self.__latmin

    def set_nullpct(self, nullpct):
        self.__nullpct = nullpct

    def generate_values(self, count):
        for i in xrange(count):
            if self.__nullpct and (random.randint(0, 100) < self.__nullpct):
                yield None
            else:
                longitude = round(self.__longmin + (self.__longdiff * random.random()),
                                  PointValueGenerator.DIGITS_BEYOND_DECIMAL_POINT)
                latitude  = round(self.__latmin  + (self.__latdiff  * random.random()),
                                  PointValueGenerator.DIGITS_BEYOND_DECIMAL_POINT)
                yield "PointFromText('POINT ("+str(longitude)+" "+str(latitude)+")')"


class PolygonValueGenerator:
    """This generates (random) polygon (GEOGRAPHY) values.
    """
    # It's annoying to have random numbers with 12 digits, so we limit it to
    # a reasonable number beyond the decimal point, but not too small since
    # too much rounding can cause invalid polygons
    DIGITS_BEYOND_DECIMAL_POINT = 6

    def __init__(self):
        decimal.getcontext().prec = PolygonValueGenerator.DIGITS_BEYOND_DECIMAL_POINT
        self.__nullpct = 0
        # A negative value indicates a random number of holes (interior rings),
        # with the number of holes ranging between 0 and the absolute value of
        # the given number; so, in this case, -4 means between 0 and 4 holes.
        self.__num_holes = -4
        # By default, polygons are restricted to be somewhere within Colorado,
        # since it has a nice, square-ish shape; and a polygon with random
        # vertices covering the entire Earth would not make a lot of sense.
        # (Note: this an approximate version of Colorado, since it does not
        # take into account that latitude lines are not great circles.)
        self.set_min_max(-109.05, -102.05, 37.0, 41.0)

    def set_min_max(self, longmin, longmax, latmin=None, latmax=None):
        self.__longmin = float(longmin)
        self.__longmax = float(longmax)
        if latmin is not None:
            self.__latmin = float(latmin)
        else:
            self.__latmin = self.__longmin
        if latmax is not None:
            self.__latmax = float(latmax)
        else:
            self.__latmax = self.__longmax

    def set_nullpct(self, nullpct):
        self.__nullpct = nullpct

    def set_count(self, num_holes):
        self.__num_holes = int(num_holes)

    def generate_vertex(self, longmin, longmax, latmin, latmax):
        """Generates a point that can be used as the vertex of a polygon, at a
           random location in between the specified minimum and maximum longitude
           and latitude values, with a small buffer so that it is not right up
           against the edge.
        """
        delta = longmax - longmin
        longitude = round(longmin + (0.1 * delta) + (0.8 * delta * random.random()),
                          PolygonValueGenerator.DIGITS_BEYOND_DECIMAL_POINT)
        delta = latmax - latmin
        latitude  = round(latmin  + (0.1 * delta) + (0.8 * delta * random.random()),
                          PolygonValueGenerator.DIGITS_BEYOND_DECIMAL_POINT)
        return str(longitude)+" "+str(latitude)

    def generate_loop(self, longmin, longmax, latmin, latmax, clockwise=False):
        """Generates a loop, which can be used as the exterior ring or an interior
           ring (i.e., a hole) of a polygon, with between 4 and 8 vertices, at
           random locations in between the specified minimum and maximum longitude
           and latitude values; clockwise=True should be used if and only if
           this is an interior ring.
        """
        # Divide the specified region up into a 3x3 grid of 9 roughly equal spaces,
        # like a tic-tac-toe board, but leave out the middle space, which can be
        # used later for holes, if this is an exterior ring. Start in the lower left
        # space (or "octant", since there are 8 of them without the middle), and
        # move counter-clockwise (the default) or clockwise until you reach the
        # lower left space again. In the corner spaces, you always choose a random
        # vertex; but the "middle" spaces are optional: you randomly decide (50-50)
        # whether to specify a vertex there or not.

        # The first octant, [0, 0], is omitted here because it is dealt with
        # specially, being both the first and last vertex
        octants = [[1, 0], [2, 0], [2, 1], [2, 2], [1, 2], [0, 2], [0, 1]]
        if clockwise:
            octants.reverse()
        long_delta = (longmax - longmin) / 3.0
        lat_delta  = (latmax  - latmin ) / 3.0
        first_and_last_vertex = self.generate_vertex(longmin, longmin+long_delta, latmin, latmin+lat_delta)
        loop = '(' + first_and_last_vertex + ', '
        for oct in range(len(octants)):
            i, j = octants[oct][0], octants[oct][1]
            # vertices in the "middle" octants are optional (unlike the corners)
            if i == 1 or j == 1 and random.randint(0, 100) < 50:
                continue
            loop += self.generate_vertex(longmin+i*long_delta, longmin+(i+1)*long_delta,
                                         latmin+j*lat_delta, latmin+(j+1)*lat_delta) + ', '
        return loop + first_and_last_vertex + ')'

    def generate_values(self, count):
        """Generates a polygon, whose first loop is always a counter-clockwise
           exterior ring, with vertices at random locations in between the
           specified minimum and maximum longitude and latitude values; there
           may or may not be additional loops which represent clockwise interior
           rings, i.e., holes. Holes are specified as being within one of 4
           quadrants of the middle "space" (see generate_loop above) of the
           exterior ring. More than 4 holes is not recommended, as they will
           start to overlap, causing an invalid polygon.
        """
        quadrants = [[0, 0], [1, 0], [1, 1], [0, 1]]
        for n in xrange(count):
            if self.__nullpct and (random.randint(0, 100) < self.__nullpct):
                yield None
            else:
                polygon = "PolygonFromText('POLYGON (" + self.generate_loop(self.__longmin, self.__longmax,
                                                                            self.__latmin, self.__latmax)
                num_holes = self.__num_holes
                if num_holes < 0:
                    num_holes = random.randint(0, -num_holes)
                if num_holes:
                    long_delta = (self.__longmax - self.__longmin) / 6.0
                    lat_delta  = (self.__latmax  - self.__latmin ) / 6.0
                    longmin    = self.__longmin + 2*long_delta
                    latmin     = self.__latmin  + 2*lat_delta
                for h in range(num_holes):
                    i, j = quadrants[h%4][0], quadrants[h%4][1]
                    polygon += ', ' + self.generate_loop(longmin+i*long_delta, longmin+(i+1)*long_delta,
                                                         latmin+j*lat_delta, latmin+(j+1)*lat_delta, True)
                yield polygon + ")')"


class StringValueGenerator:
    """This generates strings.
    """

    # Define the ASCII-only alphabet, to be used to generate strings
    ALPHABET = u"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    # For the extended, non-ASCII alphabet, add the letter e with various accents
    EXTENDED_ALPHABET = ALPHABET + u'\u00e9\u00e8\u00ea\u00eb'
    # Add some (upper & lower case) Greek characters (that do not resemble Latin letters)
    EXTENDED_ALPHABET += u'\u0393\u0394\u03b1\u03b2'
    # Add some (upper & lower case) Cyrillic (e.g. Russian) characters (that do not resemble Latin letters)
    EXTENDED_ALPHABET += u'\u0429\u042F\u0449\u044F'
    # Add some Japanese characters (which together mean 'frog')
    EXTENDED_ALPHABET += u'\u30ab\u30a8\u30eb'
    # Add some (simplified) Chinese characters (which together mean 'frog')
    EXTENDED_ALPHABET += u'\u9752\u86d9'
    # Initial, default value
    __ascii_only = False

    @staticmethod
    def set_ascii_only(ascii_only):
        StringValueGenerator.__ascii_only = ascii_only

    @staticmethod
    def get_alphabet():
        if StringValueGenerator.__ascii_only:
            return StringValueGenerator.ALPHABET
        else:
            return StringValueGenerator.EXTENDED_ALPHABET

    def __init__(self):
        self.__nullpct = 0

    def set_nullpct(self, nullpct):
        self.__nullpct = nullpct

    def generate_values(self, count, length = 14):
        for i in xrange(count):
            list = [random.choice(StringValueGenerator.get_alphabet()) for y in xrange(length)]
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
    INDF = r"IS\s+(NOT\s+)?DISTINCT\s+FROM"
    #                   token (starting with '_')
    #                   |       optional attribute section between []s
    #                   |       |
    LABEL_PATTERN_GROUP =                    "label" # optional label for variables
    #                   |       |             |
    TYPE_PATTERN_GROUP  =                                           "type" # optional type for columns, values
    #                   |       |             |                      |
    MIN_VALUE_PATTERN_GROUP =                                                                                    "min" # optional min (only for numeric values)
    #                   |       |             |                      |                                            |
    MAX_VALUE_PATTERN_GROUP =                                                                                                          "max" # optional max (only for numeric values)
    #                   |       |             |                      |                                            |                      |
    __EXPR_TEMPLATE = r"%s" r"(\[\s*" r"(#(?P<label>\w+)\s*)?" r"(?P<type>\w+|"+INDF+r"|[=<>!]{1,2})?\s*" r"(:(?P<min>(-?\d*\.?\d*)),(?P<max>(-?\d*\.?\d*))" \
                      r"(,(?P<latmin>(-?\d*\.?\d*))(,(?P<latmax>(-?\d*\.?\d*)))?)?)?(;(?P<numholes>(-?\d+)))?\s*" r"(null(?P<nullpct>(\d*)))?" r"\])?"
    #                         |                          |                                |                                  |                    |
    #                         |                          |                                |                                  |                    end of [] attribute section
    NULL_PCT_PATTERN_GROUP  =                                                                                               "nullpct" # optional null percentage
    #                         |                          |                                |
    NUM_HOLES_PATTERN_GROUP   =                                                          "numholes" # number of holes (for polygon values); or the count (for int values)
    #                         |                          |
    MAX_LAT_PATTERN_GROUP   =                           "latmax" # optional latitude max (only for geo values)
    #                         |
    MIN_LAT_PATTERN_GROUP   ="latmin" # optional latitude min (for geo values); or the step (for int values)

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

    # List of column names for Geo types, i.e., point and polygon (GEOGRAPHY_POINT and GEOGRAPHY),
    # which may need to be wrapped in AsText(...)
    __GEO_COLUMN_NAMES    = ['POINT', 'PT1', 'PT2', 'PT3', 'POLYGON', 'POLY1', 'POLY2', 'POLY3']
    # List of possible prefixes for those column names, i.e., either a table name alias with '.',
    # or nothing at all; the empty one (no table name prefix) must be last
    __GEO_COLUMN_PREFIXES = ['A.', 'B.', 'LHS.', 'SUBQ.', '']
    # List of Geo functions, which indicate that the Geo column is already appropriately
    # wrapped, so you don't need to add AsText(...)
    __GEO_FUNCTION_NAMES  = ['AREA', 'ASTEXT', 'CAST', 'CENTROID', 'CONTAINS', 'COUNT',
                             'DISTANCE', 'DWITHIN', 'ISVALID', 'ISINVALIDREASON', 'LATITUDE',
                             'LONGITUDE', 'NUMINTERIORRINGS', 'NUMPOINTS']
    # Similar list, of Geo functions with two arguments
    __GEO_FUNCS_W2_ARGS   = ['CONTAINS', 'DISTANCE', 'DWITHIN']

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
    def wrap_astext_around_geo_columns_in_fragment(cls, statement_fragment):
        """ In the specified partial SQL statement, or fragment, wrap AsText(...)
            around Geo types (point and polygon, i.e., GEOGRAPHY_POINT and
            GEOGRAPHY), but only if it is not already wrapped in one of the
            Geo functions, e.g., AsText(PT1), LONGITUDE(PT1), AREA(POLY1),
            DISTANCE(PT2,POLY3), etc.
        """
        result = statement_fragment
        statement_fragment_upper = statement_fragment.upper().replace(' ', '')
        for col in BaseGenerator.__GEO_COLUMN_NAMES:
            if col in statement_fragment_upper:
                found = False
                for tbl in BaseGenerator.__GEO_COLUMN_PREFIXES:
                    # Do not sub for empty column prefix (i.e., table
                    # name), if already handled a non-empty one
                    if found and not tbl:
                        break
                    if tbl+col in statement_fragment_upper:
                        found = True
                        if not any(f+'('+tbl+col in statement_fragment_upper for f in BaseGenerator.__GEO_FUNCTION_NAMES) and \
                           not any(f+'('+t+c+','+tbl+col in statement_fragment_upper for f in BaseGenerator.__GEO_FUNCS_W2_ARGS
                                for t in BaseGenerator.__GEO_COLUMN_PREFIXES for c in BaseGenerator.__GEO_COLUMN_NAMES):
                            result = result.replace(tbl+col, 'AsText('+tbl+col+')')
                            ### print "DEBUG: Modified fragment  : ", result
        return result


    @classmethod
    def wrap_astext_around_geo_columns(cls, statement):
        """ Cannot compare Geo types (point and polygon, i.e., GEOGRAPHY_POINT
            and GEOGRAPHY) against PostGIS, so, in a SELECT statement, we have
            to get them in text form, instead; e.g., replace 'PT1' with
            AsText(PT1) or 'A.POLY1' with AsText(A.POLY1), but only in the
            part of a SELECT statement before 'FROM', or after 'ORDER BY' (or
            between the 'THEN' part of a CASE statement and a FROM that comes
            after it), and only if it is not already wrapped in one of the Geo
            functions, e.g., AsText(PT1), LONGITUDE(PT1), AREA(POLY1),
            DISTANCE(PT2,POLY3), etc. (Note: this works for the CASE statements
            currently used in SQLCoverage, but may not for all possible CASE
            statements.)
        """
        result = statement
        statement_upper = statement.upper()
        if statement_upper.startswith('SELECT') and any(x in statement for x in BaseGenerator.__GEO_COLUMN_NAMES):
            # Normally, we will wrap AsText(...) around geo columns before FROM or after ORDER BY
            wrap_before_index  = statement_upper.find(' FROM ')
            wrap_after_index   = statement_upper.find(' ORDER BY ')
            wrap_between_index = -1
            # Special case for handling a CASE statement
            if (' CASE ' in statement_upper and ' WHEN ' in statement_upper and
                ' THEN ' in statement_upper and ' END '  in statement_upper):
                then_index = statement_upper.find(' THEN ')
                # When FROM comes after CASE/THEN, wrap AsText(...) around
                # columns that come before CASE or between THEN and FROM
                if wrap_before_index > then_index:
                    wrap_between_index = wrap_before_index
                    wrap_before_index  = statement_upper.find(' CASE ')
            if wrap_after_index > 0:
                before_text = result[0:wrap_after_index]
                after_text  = result[wrap_after_index:]
                result = before_text + BaseGenerator.wrap_astext_around_geo_columns_in_fragment(after_text)
            if wrap_between_index > 0:
                before_text  = result[0:then_index]
                between_text = result[then_index:wrap_between_index]
                after_text   = result[wrap_between_index:]
                result = before_text + BaseGenerator.wrap_astext_around_geo_columns_in_fragment(between_text) + after_text
            if wrap_before_index > 0:
                before_text = result[0:wrap_before_index]
                after_text  = result[wrap_before_index:]
                result = BaseGenerator.wrap_astext_around_geo_columns_in_fragment(before_text) + after_text
        return result

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
            # Saw the last generator, statement should be complete; now, make
            # sure Geo column types (point and polygon, i.e., GEOGRAPHY_POINT
            # and GEOGRAPHY) are not in a SELECT list (or ORDER BY) without
            # AsText, or some other function, wrapped around them
            yield BaseGenerator.wrap_astext_around_geo_columns(stmt)


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
        """ Get matching column values from schema
        """
        self.values = schema.get_typed_columns(self.__supertype)
        self.prior_generator = prior_generators.get("variable")
        prior_generators["variable"] = self # new variable generator at the head of the chain
        return prior_generators


class SymbolGenerator(BaseGenerator):
    """This replaces occurrences of token _symbol with a piece of text, such as a function name
       or a comparison operator.
       Within a statement, intended occurrences of the same symbol must use the same '#label'.
       Attributes only matter on the first occurrence of "_symbol" for a given label.
       As a convenience, forward references can use the __[#label] syntax instead of _symbol[#label]
       to defer locking in attribute settings until a later _symbol occurrence.
    """

    def __init__(self):
        BaseGenerator.__init__(self, "_symbol")

    def prepare_params(self, attribute_groups):
        # The "TYPE_PATTERN_GROUP", which in ColumnGenerator describes the column type,
        # here actually refers to a symbol, which is typically a function name or a
        # comparison operator (including the "IS [NOT] DISTINCT FROM" operator).
        self.__symbol = attribute_groups[BaseGenerator.TYPE_PATTERN_GROUP]
        if not self.__symbol:
            self.__symbol = ""

    def configure_from_schema(self, schema, prior_generators):
        """ Get matching text values; does not actually use the schema.
        """
        self.values.append(self.__symbol)
        self.prior_generator = prior_generators.get("symbol")
        prior_generators["symbol"] = self # new symbol generator at the head of the chain
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
             "timestamp": TimestampValueGenerator,
             "point": PointValueGenerator,
             "polygon": PolygonValueGenerator}

    def __init__(self):
        BaseGenerator.__init__(self, "_value")

        self.__count = COUNT
        self.__type = None

    def prepare_params(self, attribute_groups):
        self.__type = attribute_groups[BaseGenerator.TYPE_PATTERN_GROUP]
        if not self.__type:
            print "Generator parse error -- invalid type:", self.__type
            assert self.__type

        min      = attribute_groups[BaseGenerator.MIN_VALUE_PATTERN_GROUP]
        max      = attribute_groups[BaseGenerator.MAX_VALUE_PATTERN_GROUP]
        latmin   = attribute_groups[BaseGenerator.MIN_LAT_PATTERN_GROUP]
        latmax   = attribute_groups[BaseGenerator.MAX_LAT_PATTERN_GROUP]
        numholes = attribute_groups[BaseGenerator.NUM_HOLES_PATTERN_GROUP]

        self.__value_generator = ConstantGenerator.TYPES[self.__type]()

        if min is not None and max is not None:
            if latmin is not None:
                if latmax is not None:
                    self.__value_generator.set_min_max(min, max, latmin, latmax)
                else:
                    self.__value_generator.set_min_max(min, max, latmin)
            else:
                self.__value_generator.set_min_max(min, max)

        if numholes is not None:
            self.__value_generator.set_count(numholes)

        nullpct = attribute_groups[BaseGenerator.NULL_PCT_PATTERN_GROUP]
        if nullpct:
            self.__value_generator.set_nullpct(int(nullpct))


    def next_param(self):
        for i in self.__value_generator.generate_values(self.__count):
            if i == None:
                i = u"NULL"
            elif isinstance(i, basestring):
                # Points and polygon values do not want extra single-quotes around them
                if i.startswith('PointFromText(') or i.startswith('PolygonFromText('):
                    i = u"%s" % (i)
                # Varchar values do want single-quotes around them
                else:
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
        "varbinary": ("varbinary", "nonnumeric", ""),
        "timestamp": ("timestamp", "nonnumeric", ""),
        "point":     ("point",   "geo", "nonnumeric", ""),
        "polygon":   ("polygon", "geo", "nonnumeric", ""),
    }

    TYPE_NAMES = {
        FastSerializer.VOLTTYPE_TINYINT:   "byte",
        FastSerializer.VOLTTYPE_SMALLINT:  "int16",
        FastSerializer.VOLTTYPE_INTEGER:   "int32",
        FastSerializer.VOLTTYPE_BIGINT:    "int64",
        FastSerializer.VOLTTYPE_FLOAT:     "float",
        FastSerializer.VOLTTYPE_STRING:    "string",
        FastSerializer.VOLTTYPE_VARBINARY: "varbinary",
        FastSerializer.VOLTTYPE_DECIMAL:   "decimal",
        FastSerializer.VOLTTYPE_TIMESTAMP: "timestamp",
        FastSerializer.VOLTTYPE_GEOGRAPHY_POINT: "point",
        FastSerializer.VOLTTYPE_GEOGRAPHY: "polygon",
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
        self.__col_by_type["geo"] = {}
        self.__col_by_type["numeric"] = {}
        self.__col_by_type["nonnumeric"] = {}
        # This does not refer to a column type, but to columns that are part of
        # the primary key, as specified by the "indexes" key, in the schema file
        self.__col_by_type["id"] = {}
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
            indexes = tabledict.get("indexes", None)
            if indexes:
                if isinstance(indexes, basestring):
                    self.__col_by_type["id"][indexes] = table
                else:
                    for index in indexes:
                        self.__col_by_type["id"][index] = table

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
        if os.path.isfile(filename):
            fd = open(filename, "r")
        # If the file does not exist in the current directory, try the
        # adjacent 'include' directory
        else:
            fd = open('/../include/'.join(filename.rsplit('/', 1)), "r")
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
        previous_continued_line = ''
        for line in self.__lines:
            # Allow the use of '\' as a line-continuation character
            if previous_continued_line:
                line = previous_continued_line + line
            if line.endswith('\\'):
                previous_continued_line = line[:-1] + ' '
                continue
            elif line.endswith('\\\n'):
                previous_continued_line = line[:-2] + ' '
                continue
            else:
                previous_continued_line = ''
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
    def __init__(self, catalog, template, subversion_generation, ascii_only):
        StringValueGenerator.set_ascii_only(ascii_only)
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

        self.__min_statements_per_pattern = sys.maxint
        self.__max_statements_per_pattern = -1
        self.__num_insert_statements      = 0
        self.__num_unresolved_statements  = 0

    GENERATOR_TYPES = (TableGenerator, ColumnGenerator, SymbolGenerator, ConstantGenerator, IdGenerator)

    UNRESOLVED_PUNCTUATION = re.compile(r'[][#@]') # Literally, ']', '[', '#', or '@'.
    UNRESOLVED_GENERATOR = re.compile(r'(^|\W)[_]')
    # The presence of an underbar apparently inside a quoted string after the LIKE keyword
    # is likely enough to be a false positive for an unresolved generator that it is
    # allowed to pass without the usual warning triggered by a leading underbar.
    LIKELY_FALSE_ALARMS = re.compile(r"(LIKE|STARTS WITH) '[^']*_.*'")

    def __generate_statement(self, text):
        text = self.__template.apply_macros(text)
        text = unicode(text)

        print_warning = False
        for statement in self.__template.generate_statements_from_text(text):
            ### print ('VERBOSE DEBUG: text and statement post-generate_statements_from_text: "' + text + '", "' + statement + '"')
            statement, generators, field_map = BaseGenerator.prepare_generators(statement,
                                                                     self.__schema,
                                                                     SQLGenerator.GENERATOR_TYPES)
            ### print ('VERBOSE DEBUG: prepared statement looks like: "' + statement + '"')
            if (SQLGenerator.UNRESOLVED_PUNCTUATION.search(statement) or
                (SQLGenerator.UNRESOLVED_GENERATOR.search(statement) and
                 not SQLGenerator.LIKELY_FALSE_ALARMS.search(statement))):
                print_warning = True
                print ('WARNING: final statement contains suspicious unresolved symbol(s): "' +
                       statement + '"')
                print ('with schema "' + self.__schema.debug_schema_to_string() + '"')
                self.__num_unresolved_statements += 1
            for generated_stmt in BaseGenerator.generate_statements_from_list(statement,
                                                                              generators,
                                                                              field_map):
                if print_warning:
                    print ('generators:', str(generators))
                    print ('field_map :', str(field_map))
                    print ('statement :\n   ', str(statement))
                    print ('generated_stmt:\n   ', str(generated_stmt))
                yield generated_stmt
            print_warning = False

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
                upper_case_statement = i.upper().lstrip()
                if (upper_case_statement.startswith('INSERT') or upper_case_statement.startswith('UPSERT')):
                    self.__num_insert_statements += 1

            if results == 0:
                print 'Template "%s" failed to yield SQL statements' % s
            if summarize_successes:
                print '%d SQL statements generated by template:  "%s"' % (results, s)

            self.__min_statements_per_pattern = min(self.__min_statements_per_pattern, results)
            self.__max_statements_per_pattern = max(self.__max_statements_per_pattern, results)

    def min_statements_per_pattern(self):
        if (self.__min_statements_per_pattern == sys.maxint):  # initial value
            return -1  # indicates no patterns have been used to generate SQL statements
        else:
            return self.__min_statements_per_pattern

    def max_statements_per_pattern(self):
        return self.__max_statements_per_pattern

    def num_insert_statements(self):
        return self.__num_insert_statements

    def num_patterns(self):
        return len(self.__statements)

    def num_unresolved_statements(self):
        return self.__num_unresolved_statements

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
