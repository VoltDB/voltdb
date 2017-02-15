#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2017 VoltDB Inc.
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

import os.path
import re
import sys
from datetime import timedelta
from optparse import OptionParser
from random import randrange
from subprocess import Popen, PIPE, STDOUT
from time import time
from traceback import print_exc

__SYMBOL_DEFN  = re.compile(r"(?P<symbolname>[\w-]+)\s*::=\s*(?P<definition>.+)")
__SYMBOL_REF   = re.compile(r"{(?P<symbolname>[\w-]+)}")
__OPTIONAL     = re.compile(r"\[(?P<optionaltext>[^\[\]]*)\]")
__WEIGHTED_XOR = re.compile(r"\s+(?P<weight>\d*)(?P<xor>\|)\s+")
__XOR          = ' | '


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
            grammar_rule = grammar_rule[:-2] + grammar_file.readline().lstrip()
        if not grammar_rule:
            # No more grammar rules found, we're done
            break
        # Ignore blank lines and comments starting with #
        if not grammar_rule.strip() or grammar_rule.strip().startswith('#'):
            continue

        # Parse a symbol name and its definition, out of the current grammar rule
        grammar_defn = __SYMBOL_DEFN.search(grammar_rule)
        if not grammar_defn:
            print 'ERROR: Unrecognized grammar rule, ignored:\n', grammar_rule
            continue
        symbol_name = grammar_defn.group('symbolname').strip()
        definition  = grammar_defn.group('definition').strip()

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

        replacing_definition = False
        if debug:
            if symbol_name in grammar:
                replacing_definition = True
                print "WARNING: replacing definition:", symbol_name, '::=', grammar[symbol_name]

        if len(options) is 1:
            # When there are are no alternative options (i.e., no '|'),
            # the entry for the current symbol name is just a string
            grammar[symbol_name] = definition.strip()
        elif all(w is 1 for w in weights):
            # When all weights are 1 (equally weighted), the entry is a List
            grammar[symbol_name] = [options[i].strip() for i in range(len(options))]
        else:
            # When weights differ, the entry is a Dictionary, including each weight
            grammar[symbol_name] = dict([ (options[i].strip(), weights[i]) for i in range(len(options)) ])

        if debug and replacing_definition:
            print "          with new definition:", symbol_name, '::=', grammar[symbol_name]

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
            print "ERROR: Could not find symbol_name '" + str(symbol_name) + "' in grammar dictionary!!!"
            break
        # Check how deep into a recursive definition we're going
        depth[symbol_name] = depth.get(symbol_name, 0) + 1
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

    return sql.strip() + ';'


def print_file_tail(from_file, to_file, number_of_lines=50):
    """Print a tail of the last 'number_of_lines' of the 'from_file', to the
    'to_file' (typically the summary file).
    """
    command = 'tail -n ' + str(number_of_lines) + ' ' + from_file
    if debug > 2:
        print 'DEBUG: tail command:', command
        sys.stdout.flush()
    tail_proc = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    tail_message = 'Last ' + str(number_of_lines) + ' lines of ' + from_file + ':\n' \
                 + tail_proc.communicate()[0].replace('\\n', '\n') + '\n'
    print >> to_file, tail_message


def print_summary(error_message=''):
    """Prints various summary messages, to STDOUT, to the sqlcmd output file (if
    specified and not STDOUT), and to the sqlcmd summary file (if specified and
    not STDOUT); also, closes those output files, as well as the SQL statement
    output file. The summary messages may include: tails of the VoltDB server
    log file and/or of the VoltDB server's console (i.e., its STDOUT and STDERR);
    the total amount of execution time; the number and percent of valid and
    invalid SQL statements of various types, and their totals; and an error
    message, if one was specified, which usually means that execution was halted
    prematurely, due a VoltDB server crash.
    """
    global start_time, sql_output_file, sqlcmd_output_file, sqlcmd_summary_file, \
        options, count_sql_statements

    # Generate the summary message (to be printed below)
    try:
        last_sql_message = 'Last ' + str(len(last_n_sql_statements)) + ' SQL statements sent to sqlcmd:\n' \
                         + '\n'.join(sql for sql in last_n_sql_statements) + '\n'
        seconds = time() - start_time
        summary_message  = '\n\nSUMMARY: in ' + re.sub('^0:', '', str(timedelta(0, round(seconds))), 1) \
                         + ' ({0:.3f} seconds)'.format(seconds) + ', SQL statements by type:'

        # Check for a special case: TRUNCATE statements were all valid,
        # none invalid, as sometimes happens
        if count_sql_statements.get('TRUNCA') and \
                count_sql_statements.get('TRUNCA').get('invalid') is None:
            count_sql_statements['TRUNCA']['invalid'] = 0

        total_count = -1
        if count_sql_statements.get('total') and count_sql_statements.get('total').get('total'):
            total_count = count_sql_statements['total']['total']
        for sql_type in sorted(count_sql_statements):
            sql_type_count = -1
            if count_sql_statements[sql_type].get('total'):
                sql_type_count = count_sql_statements[sql_type]['total']
            summary_message += '\n    {0:6s}:'.format(sql_type)
            for validity in sorted(count_sql_statements[sql_type], reverse=True):
                if validity != 'total':  # save total for last
                    count = count_sql_statements[sql_type][validity]
                    percent = int(round(100.0 * count / sql_type_count))
                    summary_message += '{0:7d} '.format(count) + validity + ' ({0:3d}%),'.format(percent)
            percent = int(round(100.0 * sql_type_count / total_count))
            summary_message += '{0:7d} '.format(sql_type_count) + 'total ({0:3d}%)'.format(percent)
    except Exception as e:
        print '\n\nCaught exception attempting to print SUMMARY message:'
        print_exc()
        print '\n\nHere is count_sql_statements, which we were attempting to format & print:\n', count_sql_statements

    # Print the summary messages, and close output file(s)
    if sql_output_file and sql_output_file is not sys.stdout:
        sql_output_file.close()
    if sqlcmd_output_file and sqlcmd_output_file is not sys.stdout:
        print >> sqlcmd_output_file, summary_message, error_message
        sqlcmd_output_file.close()
    if sqlcmd_summary_file and sqlcmd_summary_file is not sys.stdout:
        if options.log_files and options.log_number:
            sys.stdout.flush()
            for log_file in options.log_files.split(','):
                print_file_tail(log_file, sqlcmd_summary_file, options.log_number)
        print >> sqlcmd_summary_file, last_sql_message, summary_message, error_message
        sqlcmd_summary_file.close()
    print '\n\n', last_sql_message, summary_message, error_message


def increment_sql_statement_indexes(index1, index2):
    """Increment the value of 'count_sql_statements' (a 2D dictionary, i.e.,
    a dict of dict) for the specified indexes; if there is no such value,
    initialize it to 1.
    """
    global count_sql_statements

    if not count_sql_statements.get(index1):
        count_sql_statements[index1] = {}
    if count_sql_statements[index1].get(index2):
        count_sql_statements[index1][index2] += 1
    else:
        count_sql_statements[index1][index2] = 1


def increment_sql_statement_type(type=None, validity=None):
    """Increment the value of 'count_sql_statements' (a 2D dictionary, i.e.,
    a dict of dict), both for the 'total', 'total' element and for the 'type',
    if specified (i.e., for the type, 'total' element); also, if the 'validity'
    is specified (normally equal to 'valid' or 'invalid'), increment those
    values as well (i.e., the 'total', validity and type, validity elements).
    """
    global count_sql_statements

    increment_sql_statement_indexes('total', 'total')
    if validity:
        increment_sql_statement_indexes('total', validity)
    if type:
        increment_sql_statement_indexes(type, 'total')
        if validity:
            increment_sql_statement_indexes(type, validity)


def print_sql_statement(sql, num_chars_in_sql_type=6):
    """Print the specified SQL statement (sql), to the SQL output file (which may
    be STDOUT); and, if the sqlcmd option was specified, pass that SQL statement
    to sqlcmd, and print its output in the sqlcmd output file (which may be
    STDOUT).
    """
    global sql_output_file, sqlcmd_output_file, echo_output_file, sqlcmd_proc, \
        last_n_sql_statements, options, debug

    # Print the specified SQL statement to the specified output file
    print >> sql_output_file, sql

    # If an 'echo' substring was specified, and is found in the current SQL
    # statement, then echo it to the 'echo output file': this is useful for
    # debugging things that were recently added to the SQL grammar (e.g., a
    # new function name)
    sql_contains_echo_substring = False
    if options.echo and options.echo in sql:
        sql_contains_echo_substring = True
        print >> echo_output_file, '\n' + sql

    # If a sqlcmd sub-process has been defined, use it
    if sqlcmd_proc:

        # Save the last options.summary_number SQL statements, for the summary
        last_n_sql_statements.append(sql)
        while len(last_n_sql_statements) > options.summary_number:
            last_n_sql_statements.pop(0)

        # Pass the SQL statement to the sqlcmd sub-process
        sqlcmd_proc.stdin.write(sql + '\n')
        sql_was_echoed_as_output = False

        while True:
            # Retrieve (& print) the next line of output from the sqlcmd sub-process
            output = sqlcmd_proc.stdout.readline().rstrip('\n')
            print >> sqlcmd_output_file, output

            # Debug print, if that 'echo' substring was found
            if sql_contains_echo_substring and sql_was_echoed_as_output:
                print >> echo_output_file, output

            if output:
                # Wait until the SQL statement has been echoed by sqlcmd,
                # before checking whether it was considered valid or invalid
                if output == sql:
                    sql_was_echoed_as_output = True

                # Check if sqlcmd considered the SQL statement to be valid
                elif (('(Returned ' in output and ' rows in ' in output and 's)' in output) or
                        'Command succeeded.' in output):
                    if sql_was_echoed_as_output:
                        increment_sql_statement_type(sql[0:num_chars_in_sql_type], 'valid')
                        break
                    else:  # this should never happen
                        print "\nWARNING: Unexpected condition (should never happen?!): found ", \
                              "'(Returned N rows in X.XXs)' or 'Command succeeded' before SQL echoed, ", \
                              "with:\n  sql   :", sql, '\n  output:', output

                # Check if sqlcmd considered the SQL statement to be invalid
                elif 'ERROR' in output.upper():
                    if sql_was_echoed_as_output:
                        increment_sql_statement_type(sql[0:num_chars_in_sql_type], 'invalid')
                        break
                    elif debug > 2:  # this can happen, though it's uncommon
                        print 'DEBUG: Found ERROR before SQL echoed, ', \
                              'with:\n  sql   :', sql, '\n  output:', output

                # Check if sqlcmd command not found, or if sqlcmd cannot, or
                # can no longer, reach the VoltDB server
                elif ('sqlcmd: command not found' in output
                      or 'Unable to connect' in output or 'No connections' in output
                      or 'Connection refused' in output
                      or ('Connection to database host' in output and 'was lost' in output) ):
                    error_message = '\n\n\nFATAL ERROR: sqlcmd responded:\n    "' + output + \
                                    '"\nprobably due to a VoltDB server crash (or it was ' + \
                                    'never started??), after SQL statement:\n    "' + sql + '"\n'
                    print_summary(error_message)
                    sqlcmd_proc.communicate('exit')
                    exit(99)
    else:
        increment_sql_statement_type(sql[0:num_chars_in_sql_type])


def generate_sql_statements(sql_statement_type, num_sql_statements=0, max_save_statements=1000,
                            delete_statement_type='truncate-statement', delete_statement_number=10):
    """Generate and print the specified number of SQL statements (num_sql_statements),
    of the specified type (sql_statement_type); the output file(s) should contain
    a maximum of the specified number of SQL statements (max_save_statements), meaning
    that each time we reach that number, the output file(s) are deleted and begun again.
    """
    global max_time, debug, grammar
    global count_sql_statements, sql_output_file, sqlcmd_output_file

    # A negative number of SQL statements means to run until the time limit is reached
    if num_sql_statements < 0:
        num_sql_statements = sys.maxsize

    for i in xrange(num_sql_statements):
        if max_time and time() > max_time:
            if debug > 1:
                print 'DEBUG: exceeded max_time, at:', time()
            break
        print_sql_statement(get_one_sql_statement(grammar, sql_statement_type))

        # After every 'max_save_statements' statements, delete the output file(s)
        # and start over, to avoid the file(s) becoming too large; at the same
        # time, issue TRUNCATE (or DELETE) statements, in order to avoid the
        # VoltDB server's memory growing too large
        if (count_sql_statements and count_sql_statements.get('total')
                and count_sql_statements['total'].get('total') and max_save_statements
                and not count_sql_statements['total']['total'] % max_save_statements):
            if sql_output_file and sql_output_file is not sys.stdout:
                filename = sql_output_file.name
                sql_output_file.close()
                sql_output_file = open(filename, 'w', 0)
            if sqlcmd_output_file and sqlcmd_output_file is not sys.stdout:
                filename = sqlcmd_output_file.name
                sqlcmd_output_file.close()
                sqlcmd_output_file = open(filename, 'w', 0)
            for i in range(delete_statement_number):
                print_sql_statement(get_one_sql_statement(grammar, delete_statement_type))


if __name__ == "__main__":

    # Handle command-line arguments
    parser = OptionParser()
    parser.add_option("-p", "--path", dest="path", default=".",
                      help="path to the directory in which to find the grammar file(s) to be used [default: .]")
    parser.add_option("-g", "--grammar", dest="grammar_files", default="sql-grammar.txt",
                      help="a file path/name, or comma-separated list of files, that defines the SQL grammar "
                         + "[default: sql-grammar.txt]")
    parser.add_option("-i", "--initial_type", dest="initial_type", default="insert-statement",
                      help="a type, or comma-separated list of types, of SQL statements to generate initially; typically "
                          + "used to initialize the database using INSERT statements [default: insert-statement]")
    parser.add_option("-I", "--initial_number", dest="initial_number", default=0,
                      help="the number of each 'initial_type' of SQL statement to generate [default: 0]")
    parser.add_option("-t", "--type", dest="type", default="sql-statement",
                      help="a type, or comma-separated list of types, of SQL statements to generate "
                         + "(after the initial ones, if any) [default: sql-statement]")
    parser.add_option("-n", "--number", dest="number", default=0,
                      help="the number of each 'type' of SQL statement to generate; a negative value "
                         + "means keep generating until the number of minutes is reached [default: 0; "
                         + "-1 if 'minutes' is specified]")
    parser.add_option("-m", "--minutes", dest="minutes", default=0,
                      help="the number of minutes to generate all SQL statements, of all types "
                         + "(if positive, overrides the number of SQL statements) [default: 0]")
    parser.add_option("-d", "--delete_type", dest="delete_type", default="truncate-statement",
                      help="a type of SQL statements used to delete data periodically, so that a VoltDB "
                         + "server's memory does not grow too large [default: truncate-statement]")
    parser.add_option("-N", "--delete_number", dest="delete_number", default=10,
                      help="the number of 'delete_type' SQL statements to generate, each time [default: 10]")
    parser.add_option("-x", "--max_save", dest="max_save", default=10000,
                      help="the maximum number of SQL statements (and their results, if sqlcmd is called) to save "
                         + "in the output files; after this many SQL statements, the output files are erased, and "
                         + "'delete_type' statements are called, to clear the database and start fresh [default: 10000]")
    parser.add_option("-o", "--output", dest="sql_output", default=None,
                      help="an output file path/name, to which to send all generated SQL statements; "
                         + "if not specified, output goes to STDOUT [default: None]")
    parser.add_option("-O", "--sqlcmd", dest="sqlcmd_output", default=None,
                      help="an output file path/name, to which sqlcmd output is sent, or STDOUT to send the output there; the "
                         + "generated SQL statements are only passed to sqlcmd if this value is specified [default: None]")
    parser.add_option("-s", "--summary", dest="sqlcmd_summary", default=None,
                      help="an output file path/name, to which a summary of all sqlcmd output is sent; a similar "
                         + "summary also goes to STDOUT, assuming that 'sqlcmd' is specified [default: None]")
    parser.add_option("-S", "--summary_number", dest="summary_number", default=5,
                      help="the number of SQL statements (the last ones sent to sqlcmd) to output to the 'summary' "
                         + "file; only applies if 'sqlcmd' and 'summary' are also specified [default: 5]")
    parser.add_option("-l", "--log", dest="log_files", default=None,
                      help="a file path/name, or comma-separated list of file paths/names, such as the VoltDB log "
                         + "file or console output; if this and 'summary' are specified, the summary will include "
                         + "a 'tail' of each of these files [default: None]")
    parser.add_option("-L", "--log_number", dest="log_number", default=100,
                      help="the number of lines to 'tail' from the 'log' file(s); only applies "
                         + "if 'summary' and 'log' are also specified [default: 100]")
    parser.add_option("-e", "--echo", dest="echo", default=None,
                      help="a substring to be searched for in all SQL statements sent to sqlcmd: if this is specified, "
                         + "then all SQL statements that contain this substring, and their results, will be echoed (to "
                         + "'echo_file'); this is useful for debugging new features, e.g., if you just added the LOG10 "
                         + "function, you can set this to 'LOG10', to see its effect [default: None]")
    parser.add_option("-E", "--echo_file", dest="echo_file", default=100,
                      help="a file path/name to which to send 'echo' output; if not specified, 'echo' output "
                         + "(if any) goes to STDOUT [default: None]")
    parser.add_option("-D", "--debug", dest="debug", default=0,
                      help="print debug info: 0 for none, increasing values (1-5) for more [default: 0]")
    (options, args) = parser.parse_args()

    # If 'minutes' is specified, change the default for 'number'
    if options.minutes and not options.number:
        options.number = -1

    debug = int(options.debug)
    if debug > 1:
        print "DEBUG: all arguments:", " ".join(sys.argv)
        print "DEBUG: options.path          :", options.path
        print "DEBUG: options.grammar_files :", options.grammar_files
        print "DEBUG: options.initial_type  :", options.initial_type
        print "DEBUG: options.initial_number:", options.initial_number
        print "DEBUG: options.type          :", options.type
        print "DEBUG: options.number        :", options.number
        print "DEBUG: options.minutes       :", options.minutes
        print "DEBUG: options.delete_type   :", options.delete_type
        print "DEBUG: options.delete_number :", options.delete_number
        print "DEBUG: options.max_save      :", options.max_save
        print "DEBUG: options.sql_output    :", options.sql_output
        print "DEBUG: options.sqlcmd_output :", options.sqlcmd_output
        print "DEBUG: options.sqlcmd_summary:", options.sqlcmd_summary
        print "DEBUG: options.summary_number:", options.summary_number
        print "DEBUG: options.log_files     :", options.log_files
        print "DEBUG: options.log_number    :", options.log_number
        print "DEBUG: options.echo          :", options.echo
        print "DEBUG: options.echo_file     :", options.echo_file
        print "DEBUG: options.debug         :", options.debug
        print "DEBUG: options (all):\n", options
        print "DEBUG: args (all):", args

    # If a maximum number of minutes was specified, compute the time at which
    # to stop execution
    start_time = time()
    max_time = 0
    if options.minutes:
        max_time = start_time + 60*int(options.minutes)
    if debug > 1:
        print 'DEBUG: start time:', start_time
        print 'DEBUG: max_time  :', max_time
        sys.stdout.flush()

    # Define the grammar to be used to generate SQL statements, based upon
    # the input grammar file(s)
    grammar = {}
    for grammar_file in options.grammar_files.split(','):
        grammar = get_grammar(grammar, grammar_file, options.path)

    if debug > 4:
        print 'DEBUG: grammar:'
        for key in grammar.keys():
            print '   ', key + ':', grammar[key]
        print

    # Open the output file for generated SQL statements, if specified
    sql_output_file = sys.stdout
    if options.sql_output:
        sql_output_file = open(options.sql_output, 'w', 0)

    # Open the output file for SQL statements to be echoed, if specified
    echo_output_file = None
    if options.echo:
        echo_output_file = sys.stdout
        if options.echo_file:
            echo_output_file = open(options.echo_file, 'w', 0)

    # Open the sub-process used to execute SQL statements in sqlcmd,
    # and the output file for sqlcmd results, if specified
    sqlcmd_proc = None
    sqlcmd_output_file = None
    sqlcmd_summary_file = None
    last_n_sql_statements = []
    if options.sqlcmd_output:
        if options.sqlcmd_output is "STDOUT":
            sqlcmd_output_file = sys.stdout
        else:
            sqlcmd_output_file = open(options.sqlcmd_output, 'w', 0)

        if options.sqlcmd_summary:
            sqlcmd_summary_file = open(options.sqlcmd_summary, 'w', 0)

        command = 'sqlcmd --stop-on-error=false'
        if debug > 2:
            print 'DEBUG: sqlcmd command:', command
        sqlcmd_proc = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)

    # Generate the specified number of each type of SQL statement;
    # and run each in sqlcmd, if the sqlcmd option was specified
    count_sql_statements = {}
    if options.initial_number:
        for sql_statement_type in options.initial_type.split(','):
            generate_sql_statements(sql_statement_type, int(options.initial_number))
    for sql_statement_type in options.type.split(','):
        generate_sql_statements(sql_statement_type, int(options.number), int(options.max_save),
                                options.delete_type, options.delete_number)

    if debug > 3:
        print_sql_statement('select * from P1;')
        print_sql_statement('select * from R1;')
        print_sql_statement('select ID, TINY, SMALL, INT, BIG, NUM, DEC, VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, TIME from P1;')
        print_sql_statement('select ID, TINY, SMALL, INT, BIG, NUM, DEC, VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, TIME from R1;')
        print_sql_statement('select count(ID), count(TINY), count(SMALL), count(INT), count(BIG), count(NUM), count(DEC), count(VCHAR), count(VCHAR_INLINE_MAX), count(VCHAR_INLINE), count(TIME), count(VARBIN), count(POINT), count(POLYGON) from P1;')
        print_sql_statement('select count(ID), count(TINY), count(SMALL), count(INT), count(BIG), count(NUM), count(DEC), count(VCHAR), count(VCHAR_INLINE_MAX), count(VCHAR_INLINE), count(TIME), count(VARBIN), count(POINT), count(POLYGON) from R1;')

    if debug > 1:
        print 'DEBUG: end time  :', time()

    print_summary()

    # Exit sqlcmd, if it was opened
    if sqlcmd_proc:
        sqlcmd_proc.communicate('exit')
