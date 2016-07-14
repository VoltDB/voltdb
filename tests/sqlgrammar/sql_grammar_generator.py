
import sys
import os.path
import re
import subprocess
import time
from datetime import timedelta
from optparse import OptionParser
from random import randrange
from time import time

__SYMBOL_DEFN  = re.compile(r"(?P<symbolname>[\w-]+)\s*::=\s*(?P<definition>.+)")
__SYMBOL_REF   = re.compile(r"{(?P<symbolname>[\w-]+)}")
__OPTIONAL     = re.compile(r"\[(?P<optionaltext>[^\[\]]*)\]")
__WEIGHTED_XOR = re.compile(r"\s+(?P<weight>\d*)(?P<xor>\|)\s+")
__XOR         = ' | '


def get_grammar(grammar={}, grammar_filename='sql-grammar.txt', grammar_dir='.'):
    """Reads the SQL grammar from the specified file, in the specified directory.
    """

    global debug

    grammar_path = os.path.abspath(os.path.join(grammar_dir, grammar_filename))
    grammar_file = open(grammar_path, 'r')

    while True:
        grammar_rule = grammar_file.readline()
        # If a line ends with '\', combine it with the next line
        while grammar_rule.endswith('\\\n'):
            grammar_rule = grammar_rule[:-2] + grammar_file.readline()
        if not grammar_rule:
            # No more grammar rules found, we're done
            break
        # Ignore blank lines and comments starting with #
        if not grammar_rule.strip() or grammar_rule.strip().startswith('#'):
            continue

        # Parse a symbol name and its definition, out of the current grammar rule
        grammar_defn = __SYMBOL_DEFN.search(grammar_rule)
        if not grammar_defn:
            print 'Unrecognized grammar rule, ignored:', grammar_rule
            continue
        symbol_name = grammar_defn.group('symbolname').strip()
        definition  = grammar_defn.group('definition').strip()

        weighted_xor = re.search(__WEIGHTED_XOR, definition)
        weights = []
        for wxor in __WEIGHTED_XOR.finditer(definition):
            weight = wxor.group('weight')
            if weight:
                weights.append(int(weight))
                definition = definition.replace(weight+'|', '|', 1)
            else:
                weights.append(1)
        # Last item has a weight of 1, by definition
        weights.append(1)
        options = definition.split(__XOR)
        if len(options) is 1:
            # When there are are no alternative options (i.e., no '|'),
            # the entry for the current symbol name is just a String
            grammar[symbol_name] = definition.strip()
        elif all(w is 1 for w in weights):
            # When all weights are 1 (equally weighted), the entry is a List
            grammar[symbol_name] = [options[i].strip() for i in range(len(options))]
        else:
            # When weights differ, the entry is a Dictionary, including each weight
            grammar[symbol_name] = dict([ (options[i].strip(), weights[i]) for i in range(len(options)) ])

    return grammar


def get_one_sql_statement(grammar, sql_statement_type='sql-statement', max_depth=5, optional_percent=50):
    """Randomly generates one SQL statement of the specified type, using the
       specified maximum depth (meaning that recursive definitions are limited
       to that depth) and optional percent (meaning that option clauses, in
       brackets, have that percentage chance of being used).
    """
    sql = '{' + sql_statement_type + '}'
#     print 'DEBUG: sql:', sql

    max_count = 10000
    count = 0
    depth = {}
    symbol = __SYMBOL_REF.search(sql)
    while symbol and count < max_count:
        count += 1
        bracketed_name = symbol.group(0)
        symbol_name = symbol.group('symbolname')
        definition  = grammar.get(symbol_name)
        #print 'DEBUG: bracketed_name:', str(bracketed_name)
        #print 'DEBUG: symbol_name :', str(symbol_name)
        #print 'DEBUG: definition:', str(definition)
        if not definition:
            print "Could not find symbol_name '" + str(symbol_name) + "' in grammar dictionary!!!"
            break
        # Check how deep into a recursive definition we're going
        if depth.get(symbol_name):
            depth[symbol_name] += 1
        else:
            depth[symbol_name] = 1
        if isinstance(definition, list):
            random_index = randrange(0, len(definition))
            #print 'DEBUG: len(definition):', str(len(definition))
            #print 'DEBUG: random_index:', str(random_index)
            # Avoid going too deep into a recursive definition, if possible:
            # if there are alternatives, pick one
            if (depth[symbol_name] > max_depth and bracketed_name in definition[random_index] and
                    any(bracketed_name not in definition[i] for i in range(len(definition)) ) ):
                while bracketed_name in definition[random_index]:
                    random_index = randrange(0, len(definition))
                    #print 'DEBUG: random_index:', str(random_index)
            definition = definition[random_index]
        elif isinstance(definition, dict):
            random_selection = randrange(0, sum(definition.values()))
            # TODO: combine the max depth stuff above with the weights thing below ?!?
            sum_weights = 0
            for key in definition:
                sum_weights += definition[key]
                if random_selection < sum_weights:
                    definition = key
                    break
        #print 'DEBUG: definition:', definition
        # Check for any optional text [in brackets], and decide whether to include it or not
        optional = __OPTIONAL.search(definition)
        while optional:
            #print 'DEBUG: optional:', str(optional)
            bracketed_optionaltext = optional.group(0)
            optionaltext = optional.group('optionaltext')
            #print 'DEBUG: bracketed_optionaltext:', bracketed_optionaltext
            #print 'DEBUG: optionaltext:', optionaltext
            if randrange(0, 100) < optional_percent:
                definition = definition.replace(bracketed_optionaltext, optionaltext, 1)
            else:
                definition = definition.replace(bracketed_optionaltext, '', 1)
            #print 'DEBUG: definition:', definition
            optional = __OPTIONAL.search(definition)
        sql = sql.replace(bracketed_name, definition, 1)
        #print 'DEBUG: sql:', sql

        symbol = __SYMBOL_REF.search(sql)

    if count >= max_count:
        print "Gave up after", count, "iterations: possible infinite loop in grammar dictionary!!!"

    return sql + ';'


def print_summary(output_file=sys.stdout):
    """TBD
    """
    global start_time, count_all_sql_statements
    seconds = time() - start_time

    summary_message = '\n\nSUMMARY: ' + str(count_all_sql_statements) + ' SQL statements, in ' + \
            re.sub("^0:", "", str(timedelta(0, round(seconds))), 1) + " ({0:.3f} seconds)".format(seconds)

    print >> output_file, summary_message
    if output_file is not sys.stdout:
        print summary_message


def print_sql_statement(sql, output_file=sys.stdout, sqlcmd=None, debug=0):
    """TBD
    """

    if not sqlcmd:
        print >> output_file, sql

    else:   # Pipe the SQL statement to sqlcmd
        if debug > 4:
            print >> output_file, 'DEBUG: SQL statement:', sql
        sqlcmd.stdin.write(sql + '\n')
        while True:
            output = sqlcmd.stdout.readline()
            print >> output_file, output.rstrip('\n')

            # TODO: might want to use regex's here:
            if '(Returned ' in output and ' rows in ' in output and 's)' in output:
                if debug > 4:
                    print >> output_file, 'DEBUG: FOUND: (Returned ... rows in ...s)'
                break
            elif 'ERROR' in output or 'Error' in output:
                if debug > 4:
                    print >> output_file, 'DEBUG: FOUND: ERROR or Error'
                break
            elif (not output or 'Unable to connect' in output or 'No connections' in output
                   or 'Connection refused' in output
                   or ('Connection to database host' in output and 'was lost' in output) ):
                error_message = '\n\nFATAL ERROR: sqlcmd responded:\n    "' + output.rstrip('\n') + '"\npossibly due to ' + \
                                'a VoltDB server crash (or it was never started), after SQL statement:\n    "' + sql + '"'
                print >> output_file, error_message
                if output_file is not sys.stdout:
                    print error_message
                print_summary()
                sqlcmd.communicate('exit')
                exit(99)


if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("-p", "--path", dest="path", default=".",
                      help="path to the directory in which to find the grammar file(s) to be used")
    parser.add_option("-g", "--grammar", dest="grammar_file", default="sql-grammar.txt",
                      help="the name of the (primary) grammar file to be used")
    parser.add_option("-m", "--modified", dest="modified_grammar_file", default=None,
                      help="the name of an additional grammar file to be used, to modify the (primary) grammar")
    parser.add_option("-i", "--insert", dest="insert", default=10,
                      help="the number of INSERT SQL statements to generate, at the beginning")
    parser.add_option("-u", "--update", dest="update", default=0,
                      help="the number of UPDATE SQL statements to generate, after the inserts (if any)")
    parser.add_option("-v", "--upsert", dest="upsert", default=0,
                      help="the number of UPSERT SQL statements to generate, after the updates (if any)")
    parser.add_option("-q", "--select", dest="select", default=0,
                      help="the number of SELECT queries to generate, after the upserts (if any)")
    parser.add_option("-d", "--delete", dest="delete", default=0,
                      help="the number of DELETE (or TRUNCATE) SQL statements to generate, after the selects (if any)")
    parser.add_option("-a", "--sql", dest="sql_any", default=10,
                      help="the number of SQL statements, of arbitrary / all types, to produce, at the end")
    parser.add_option("-t", "--time", dest="minutes", default=0,
                      help="the number of minutes to produce SQL statements, of any type, at the end (overrides the number of statements)")
    parser.add_option("-s", "--sqlcmd", dest="sqlcmd", default=None,
                      help="the command to be used to call sqlcmd, e.g. 'sqlcmd', but could also include a path; " + \
                           "when this is specified, all SQL statements generated will also be passed to sqlcmd")
    parser.add_option("-o", "--output", dest="output", default=None,
                      help="an output file name, to which to send SQL statements or sqlcmd output")
    parser.add_option("-D", "--debug", dest="debug", default=0,
                      help="print debug info: 0 for none, increasing values for more")
    (options, args) = parser.parse_args()

    debug = int(options.debug)
    if debug > 1:
        print "DEBUG: all arguments  :", " ".join(sys.argv)
        print "DEBUG: options.path   :", options.path
        print "DEBUG: grammar_file   :", options.grammar_file
        print "DEBUG: modified_grammar_file:", options.modified_grammar_file
        print "DEBUG: options.insert :", options.insert
        print "DEBUG: options.update :", options.update
        print "DEBUG: options.upsert :", options.upsert
        print "DEBUG: options.select :", options.select
        print "DEBUG: options.delete :", options.delete
        print "DEBUG: options.sqlany :", options.sql_any
        print "DEBUG: options.minutes:", options.minutes
        print "DEBUG: options.output :", options.output
        print "DEBUG: options.sqlcmd :", options.sqlcmd
        print "DEBUG: options.debug  :", options.debug
        print "DEBUG: options (all)  :\n", options
        print "DEBUG: args (all)     :", args

    start_time = time()
    max_time = sys.maxsize
    num_sql_any = int(options.sql_any)
    if options.minutes:
        max_time = start_time + 60*int(options.minutes)
        num_sql_any = sys.maxsize
    if debug:
        print 'DEBUG: start time     :', start_time
        print 'DEBUG: max_time       :', max_time
        print 'DEBUG: num_sql_any    :', num_sql_any

    #TODO: temp debug:
#     grammar = get_grammar({}, 'temp-test-grammar.txt')
#     print 'DEBUG: grammar:'
#     for key in grammar.keys():
#         print '   ', key + ':', grammar[key]
#     print '----------------------------------------'
#     for i in range(20):
#         sql = get_one_sql_statement(grammar, 'num-column-name')
#         print_sql_statement(sql)
#     print '----------------------------------------'
#     for i in range(20):
#         sql = get_one_sql_statement(grammar, 'int-column-name')
#         print_sql_statement(sql)
#     print '----------------------------------------'
#     for i in range(20):
#         sql = get_one_sql_statement(grammar, 'foo-column-name')
#         print_sql_statement(sql)
#     print '----------------------------------------'
#     for i in range(20):
#         sql = get_one_sql_statement(grammar, 'bar-column-name')
#         print_sql_statement(sql)
#     exit()

    # Define the grammar to be used to generate SQL statements
    grammar = get_grammar({}, options.grammar_file, options.path)
    if options.modified_grammar_file:
        grammar = get_grammar(grammar, options.modified_grammar_file, options.path)

    if debug > 5:
        print 'DEBUG: grammar:'
        for key in grammar.keys():
            print '   ', key + ':', grammar[key]
        print

    output_file = sys.stdout
    if options.output:
        output_file = open(options.output, 'w')

    sqlcmd = None
    if options.sqlcmd:
        command = options.sqlcmd + ' --stop-on-error=false'
#         if options.output:
#             command += ' > ' + options.output
        if debug > 2:
            print 'DEBUG: sqlcmd command:', command
        sqlcmd = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # Run the specified number of INSERT statements
    count_all_sql_statements = 0
    for i in xrange(int(options.insert)):
        sql = get_one_sql_statement(grammar, 'insert-statement')
        print_sql_statement(sql, output_file, sqlcmd, debug)
        count_all_sql_statements += 1

    # Run the specified number of UPSERT statements
    for i in xrange(int(options.update)):
        sql = get_one_sql_statement(grammar, 'update-statement')
        print_sql_statement(sql, output_file, sqlcmd, debug)
        count_all_sql_statements += 1

    # Run the specified number of UPSERT statements
    for i in xrange(int(options.upsert)):
        sql = get_one_sql_statement(grammar, 'upsert-statement')
        print_sql_statement(sql, output_file, sqlcmd, debug)
        count_all_sql_statements += 1

    # Run the specified number of SELECT statements
    for i in xrange(int(options.select)):
        sql = get_one_sql_statement(grammar, 'select-statement')
        print_sql_statement(sql, output_file, sqlcmd, debug)
        count_all_sql_statements += 1

    # Run the specified number of SELECT statements
    for i in xrange(int(options.delete)):
        sql = get_one_sql_statement(grammar, 'delete-statement')
        print_sql_statement(sql, output_file, sqlcmd, debug)
        count_all_sql_statements += 1

    # Run the specified number of (non-DDL) SQL statements, of any type
    for i in xrange(num_sql_any):
        sql = get_one_sql_statement(grammar)
        print_sql_statement(sql, output_file, sqlcmd, debug)
        count_all_sql_statements += 1
        if time() > max_time:
            if debug:
                print 'DEBUG: exceeded max_time, at:', time()
            break

    if debug > 3:
        print_sql_statement('select * from P1;', output_file, sqlcmd, debug)
        print_sql_statement('select * from R1;', output_file, sqlcmd, debug)
        print_sql_statement('select ID, TINY, SMALL, INT, BIG, NUM, DEC, VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, TIME from P1;', output_file, sqlcmd, debug)
        print_sql_statement('select ID, TINY, SMALL, INT, BIG, NUM, DEC, VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, TIME from R1;', output_file, sqlcmd, debug)
        print_sql_statement('select count(ID), count(TINY), count(SMALL), count(INT), count(BIG), count(NUM), count(DEC), count(VCHAR), count(VCHAR_INLINE_MAX), count(VCHAR_INLINE), count(TIME), count(VARBIN), count(POINT), count(POLYGON) from P1;', output_file, sqlcmd, debug)
        print_sql_statement('select count(ID), count(TINY), count(SMALL), count(INT), count(BIG), count(NUM), count(DEC), count(VCHAR), count(VCHAR_INLINE_MAX), count(VCHAR_INLINE), count(TIME), count(VARBIN), count(POINT), count(POLYGON) from R1;', output_file, sqlcmd, debug)

    if debug:
        print 'DEBUG: end time       :', time()

    print_summary()

    # Exit sqlcmd, if it was opened
    if sqlcmd:
        sqlcmd.communicate('exit')
