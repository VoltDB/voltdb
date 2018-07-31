#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.
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
from random import randrange, seed
from signal import alarm, signal, SIGALRM
from subprocess import Popen, PIPE, STDOUT
from time import time, localtime, strftime
from traceback import print_exc

__SYMBOL_DEFN      = re.compile(r"(?P<symbolname>[\w-]+)\s*::=\s*(?P<definition>.*)")
__SYMBOL_REF       = re.compile(r"{(?P<symbolname>[\w-]+)}")
__SYMBOL_REF_REUSE = re.compile(r"{(?P<symbolname>[\w-]*):(?P<reusename>[\w-]+)}")
__SYMBOL_REF_EXIST = re.compile(r"{(?P<symbolname>[\w-]*);(?P<existingnames>[\w,-]+)}")
__OPTIONAL         = re.compile(r"(?<!\\)\[(?P<optionaltext>[^\[\]]*[^\[\]\\]?)\]")
__WEIGHTED_XOR     = re.compile(r"\s+(?P<weight>\d*)(?P<xor>\|)\s+")
__XOR              = ' | '
__DATE_TIME        = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}")


def get_grammar(grammar={}, grammar_filename='sql-grammar.txt', grammar_dir='.'):
    """Reads the SQL grammar from the specified file, in the specified directory.
    """

    global debug

    grammar_path = os.path.abspath(os.path.join(grammar_dir, grammar_filename))
    grammar_file = open(grammar_path, 'r')

    while True:
        grammar_rule = grammar_file.readline()

        # No more grammar rules found, we're done
        if not grammar_rule:
            break
        # Ignore blank lines and comments starting with #
        if not grammar_rule.strip() or grammar_rule.strip().startswith('#'):
            continue
        # If a line ends with '\', combine it with the next line
        while grammar_rule.endswith('\\\n'):
            next_line = grammar_file.readline().lstrip()
            if not next_line.startswith('#'):
                grammar_rule = grammar_rule[:-2] + next_line

        # Parse a symbol name and its definition, out of the current grammar rule
        grammar_defn = __SYMBOL_DEFN.search(grammar_rule)
        if not grammar_defn:
            print 'ERROR: Unrecognized grammar rule, ignored:\n', grammar_rule
            continue
        symbol_name = grammar_defn.group('symbolname').strip()
        definition  = grammar_defn.group('definition').strip()

        # Check for use of '::=' within the definition, which is an error
        if '::=' in definition:
            print "\n\nFATAL ERROR: Definition of '" + str(symbol_name) + "' in grammar " + \
                  "dictionary contains illegal characters '::=':\n    " + str(definition)
            exit(21)

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
                print "WARNING: replacing definition:", symbol_name, ' ::= ', grammar[symbol_name]

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
            print "          with new definition:", symbol_name, ' ::= ', grammar[symbol_name]

    return grammar


def get_one_sql_statement(grammar, sql_statement_type='sql-statement', max_depth=5,
                          optional_percent=50, symbol_reuse={}, final_statement=True):
    """Randomly generates one SQL statement of the specified type, using the
       specified maximum depth (meaning that recursive definitions are limited
       to that depth) and optional percent (meaning that option clauses, in
       brackets, have that percentage chance of being used).
    """
    global symbol_depth, symbol_order, options, debug

    sql = '{' + sql_statement_type + '}'

    max_count = 10000
    count = 0
    if final_statement:
        symbol_order = []
        symbol_depth = {}
        symbol_reuse = {}
    symbol = __SYMBOL_REF_REUSE.search(sql) or __SYMBOL_REF.search(sql) or __SYMBOL_REF_EXIST.search(sql)
    while symbol and count < max_count:
        count += 1
        bracketed_name = symbol.group(0)
        symbol_name = symbol.group('symbolname')
        symbol_opts = None
        existing_names = []
        reuse_name = None
        reuse_value = None
        try:
            symbol_opts = symbol.group('existingnames')
            existing_names = symbol_opts.split(',')
        except IndexError as ex:
            pass
        if existing_names:
            remove_names = []
            for en in existing_names:
                if symbol_reuse.get(en) is None:
                    remove_names.append(en)
            for en in remove_names:
                existing_names.remove(en)
            last = len(existing_names) - 1
            for en in existing_names:
                if (en is existing_names[last] or
                        randrange(0, 100) < optional_percent):
                    reuse_name = en
                    break
        else:
            try:
                reuse_name = symbol.group('reusename')
            except IndexError as ex:
                reuse_name = None
        definition = grammar.get(symbol_name)
        if debug > 6:
            print 'DEBUG: sql           :', str(sql)
            print 'DEBUG: symbol_reuse  :', str(symbol_reuse)
            print 'DEBUG: bracketed_name:', str(bracketed_name)
            print 'DEBUG: symbol_name   :', str(symbol_name)
            print 'DEBUG: symbol_opts   :', str(symbol_opts)
            print 'DEBUG: existing_names:', str(existing_names)
            print 'DEBUG: reuse_name    :', str(reuse_name)
            print 'DEBUG: definition    :', str(definition)

        # Handle the case where the same symbol could be used twice (or more)
        # in the same SQL statement, e.g., {table-name:t1} can be reused to
        # refer to the same table name more than once; the shorter {:t1} may
        # also be used, after the first occurrence
        if reuse_name:
            if symbol_reuse.get(reuse_name) is not None:
                # This reuse_name has been used before, so replace all
                # occurrences of it with the same value used before
                reuse_value = symbol_reuse[reuse_name]
            else:
                # This reuse_name has not been used before, so choose a value
                # that will be used to replace it, throughout this SQL statement
                reuse_value = get_one_sql_statement(grammar, symbol_name, max_depth,
                                                    optional_percent, symbol_reuse, False)
                symbol_reuse[reuse_name] = reuse_value

            sql = sql.replace(bracketed_name, reuse_value)
            symbol = __SYMBOL_REF_REUSE.search(sql) or __SYMBOL_REF.search(sql) or __SYMBOL_REF_EXIST.search(sql)

            # For debugging purposes, we may wish to track symbol use
            if options.echo_grammar and symbol_name:
                symbol_order.append((symbol_name, sql, symbol_opts, str(existing_names), reuse_name, reuse_value))
            continue

        # For debugging purposes, we may wish to track symbol use
        if options.echo_grammar and symbol_name:
            symbol_order.append((symbol_name, sql, symbol_opts, str(existing_names), reuse_name, reuse_value))

        if definition is None:
            print "\n\nFATAL ERROR: Could not find definition of '" + str(symbol_name) + \
                  "' in grammar dictionary!!!"
            exit(22)

        # Check how deep into a recursive definition we're going
        if symbol_depth.get(symbol_name):
            symbol_depth[symbol_name] += 1
        else:
            symbol_depth[symbol_name] = 1

        if isinstance(definition, list):
            random_index = randrange(0, len(definition))
            #print 'DEBUG: len(definition):', str(len(definition))
            #print 'DEBUG: random_index:', str(random_index)
            # Avoid going too deep into a recursive definition, if possible:
            # if there are alternatives, pick one
            if (symbol_depth[symbol_name] > max_depth and bracketed_name in definition[random_index] and
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

        symbol = __SYMBOL_REF_REUSE.search(sql) or __SYMBOL_REF.search(sql) or __SYMBOL_REF_EXIST.search(sql)

    if count >= max_count:
        print "Gave up after", count, "iterations: possible infinite loop in grammar dictionary!!!"
        if debug:
            if bracketed_name:
                print "DEBUG: bracketed_name:", bracketed_name
            if symbol_name:
                print "DEBUG: symbol_name   :", symbol_name
            if symbol_name:
                print "DEBUG: symbol_opts   :", symbol_opts
            if existing_names:
                print "DEBUG: existing_names:", str(existing_names)
            if definition:
                print "DEBUG: definition    :", definition
        if debug > 5:
            print "DEBUG: sql:", sql.strip(), "\n"
            print "DEBUG: symbol_depth:\n", symbol_depth, "\n\n"
            if symbol_order:
                print "DEBUG: symbol_order:\n", symbol_order, "\n\n"

    if final_statement:
        sql = sql.strip().replace('\\\\', '<real-backslash>') \
                         .replace('\[', '[').replace('\]', ']').replace('\{', '{').replace('\}', '}') \
                         .replace('<real-backslash>', '\\') + ';'
        if debug > 6:
            print "DEBUG: final sql     :", sql.strip(), "\n"

    return sql


def add_suffix_to_file_name(file_name, suffix=None):
    """Adds the specified suffix, if any, either to the end of the specified
    file_name; or, if the file_name has an "extension" starting with a dot ('.'),
    then just in front of the dot.
    """
    result = file_name
    if suffix:
        ext_index = str(file_name).rfind('.')
        if ext_index > -1:
            file_name = file_name[:ext_index] + suffix + file_name[ext_index:]
        else:
            file_name += suffix
    return file_name


def print_file_tail_and_errors(from_file, to_file, number_of_lines=50):
    """Print a 'tail' of the last 'number_of_lines' of the 'from_file', to the
    'to_file' (typically the summary file); and a 'grep' of the number of
    various types of error messages. Also, if (Java) Exceptions or other ERRORs
    are found in the file, print 2 files, containing a list of all the distinct
    errors (1 file for Java Exceptions, 1 for ERROR messages), and how many
    times they appeared.
    """
    command = 'tail -n ' + str(number_of_lines) + ' ' + from_file
    if debug > 4:
        print 'DEBUG: tail command:', command
        sys.stdout.flush()
    tail_proc = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    tail_message = '\n\nLast ' + str(number_of_lines) + ' lines of ' + from_file + ':\n' \
                 + tail_proc.communicate()[0].replace('\\n', '\n')

    # Various types of errors we're interested in finding (in the log or
    # console output), divided into several groups, i.e., (Java) Exceptions
    # vs. generic errors vs. warnings
    error_types = [{'type':'Exception', 'title':"Java 'Exception'",
                    'subtypes':['NullPointerException',
                                'ArrayIndexOutOfBoundsException',
                                'IndexOutOfBoundsException',
                                'ClassCastException',
                                'SAXParseException',
                                'RuntimeException',
                                'VoltTypeException']},
                    {'type':'ERROR', 'title':"'ERROR'",
                    'subtypes':['Error compiling query',
                                'ERROR: IN: NodeSchema',
                                'is already defined',
                                'is not defined',
                                'Could not parse reader',
                                'unexpected token: EXEC',
                                'Failed to plan for statement',
                                'Resource limit exceeded',
                                "Attribute 'enabled' is not allowed to appear in element 'export'",
                                'PartitionInfo specifies invalid parameter index for procedure',
                                'Unexpected error in the SQL parser for statement']},
                     {'type':'WARN', 'title':"'WARN'",
                    'subtypes':['No index found to support UPDATE and DELETE on some of the min() / max() columns in the materialized view']}
                   ]
    # Avoid double-counting: ArrayIndexOutOfBoundsException is also counted as
    # IndexOutOfBoundsException; 'unexpected token: EXEC' is also counted as
    # 'Failed to plan for statement'
    error_subtypes_contained_in_others = ['ArrayIndexOutOfBoundsException', 'unexpected token: EXEC']

    error_count = 0
    total_error_count = 0
    total_exception_count = 0
    grand_total_count = 0
    tail_message += ''
    for et in error_types:
        error_type = et['type']
        tail_message += '\n\n'+et['title']+' messages:'
        for subtype in et['subtypes'] + [error_type]:
            command = 'grep -c "' + subtype + '" ' + from_file
            if debug > 4:
                print 'DEBUG: grep command:', command
                sys.stdout.flush()
            grep_proc = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
            try:
                num_errors = int(grep_proc.communicate()[0].replace('\\n', ''))
            except Exception as ex:
                tail_message += "Error reading file '"+from_file+"':\n   " + str(ex)
                print "\nError reading file '"+from_file+"':\n   " + str(ex)
                break
            if subtype is error_type:
                tail_message += '\nNumber of other {0:24s} messages: {1:7d}'.format("'"+subtype+"'", num_errors - error_count)
                tail_message += '\nTotal Number of {0:24s} messages: {1:7d}'.format("'"+subtype+"'", num_errors)
                error_count = 0
                if error_type is 'Exception':
                    total_exception_count = num_errors
                elif error_type is 'ERROR':
                    total_error_count = num_errors
                grand_total_count += num_errors
            else:
                tail_message += '\nNumber of {0:32s} errors: {1:7d}'.format("'"+subtype+"'", num_errors)
                # Avoid double-counting redundant sub-types (see above)
                if subtype not in error_subtypes_contained_in_others:
                    error_count += num_errors
    tail_message += '\nGrand Total of Exception/ERROR/WARN      messages: {0:7d}'.format(grand_total_count)
    print >> to_file, tail_message

    # Print 2 files containing lists of all of the (Java) Exceptions and ERRORs
    # listed in 'from_file', if any
    if total_error_count + total_exception_count:
        input_file_name = from_file[from_file.rfind('/') + 1:]
        errors_and_types = []
        exceptions_and_types = []
        error_output_file = None
        exception_output_file = None
        if total_error_count:
            error_output_file = open(add_suffix_to_file_name('errors_in_'+input_file_name, options.suffix), 'w', 0)
        if total_exception_count:
            exception_output_file = open(add_suffix_to_file_name('exceptions_in_'+input_file_name, options.suffix), 'w', 0)
        read_from_file = open(from_file, 'r')

        current_error = None
        current_exception = None
        current_error_type = None
        current_exception_type = None
        while True:
            from_file_line = read_from_file.readline()
            if __DATE_TIME.search(from_file_line):
                # Keep the date, but replace the time with a generic string
                from_file_line = from_file_line[:11] + 'hh:mm:ss,xxx' + from_file_line[23:]
            if not from_file_line:
                break
            if current_exception:
                if from_file_line.lstrip().startswith('at '):
                    current_exception += from_file_line
                    if not current_error:
                        continue
                else:  # Reached the end of an exception
                    foundType = False
                    for extype in exceptions_and_types:
                        if current_exception_type == extype['exception-type']:
                            foundType = True
                            extype['count'] += 1
                            foundException = False
                            for ex in extype['exceptions']:
                                if current_exception == ex['exception']:
                                    foundException = True
                                    ex['count'] += 1
                                    break
                            if not foundException:
                                extype['exceptions'].append({'exception':current_exception, 'count':1})
                    if not foundType:
                        exceptions_and_types.append({'exception-type':current_exception_type, 'count':1,
                                                     'exceptions':[{'exception':current_exception, 'count':1}]})
                    current_exception_type = None
                    current_exception = None
            if current_error:
                if ('ERROR' in from_file_line or 'Exception' in from_file_line
                        or from_file_line.lstrip().startswith('at ')):
                    current_error += from_file_line
                else:  # Reached the end of an ERROR
                    foundType = False
                    for ertype in errors_and_types:
                        if current_error_type == ertype['error-type']:
                            foundType = True
                            ertype['count'] += 1
                            foundError = False
                            for er in ertype['errors']:
                                if current_error == er['error']:
                                    foundError = True
                                    er['count'] += 1
                                    break
                            if not foundError:
                                ertype['errors'].append({'error':current_error, 'count':1})
                    if not foundType:
                        errors_and_types.append({'error-type':current_error_type, 'count':1,
                                                 'errors':[{'error':current_error, 'count':1}]})
                    current_error_type = None
                    current_error = None
            for et in error_types:
                error_type = et['type']
                for subtype in et['subtypes'] + [error_type]:
                    if subtype in from_file_line:
                        if error_type is 'Exception':
                            current_exception_type = subtype
                            current_exception = from_file_line
                            break
                        elif error_type is 'ERROR':
                            current_error_type = subtype
                            current_error = from_file_line
                            break

        print >> exception_output_file, 'Exceptions found in', from_file + ':'
        for extype in sorted(exceptions_and_types, key=lambda ext: ext['count'], reverse=True):
            print >> exception_output_file, '\nException type: "' + extype['exception-type'] \
                    + '"(total count:', str(extype['count']) + '):'
            for ex in sorted(extype['exceptions'], key=lambda e: e['count'], reverse=True):
                times = 'times:\n'
                if ex['count'] == 1:
                    times = 'time:\n'
                print >> exception_output_file, '\nFound', str(ex['count']), times + ex['exception']

        print >> error_output_file, 'ERRORs found in', from_file + ':'
        for ertype in sorted(errors_and_types, key=lambda ert: ert['count'], reverse=True):
            print >> error_output_file, '\nError type: "' + ertype['error-type'] \
                    + '" (total count:', str(ertype['count']) + '):'
            for er in sorted(ertype['errors'], key=lambda e: e['count'], reverse=True):
                times = 'times:\n'
                if er['count'] == 1:
                    times = 'time:\n'
                print >> error_output_file, '\nFound', str(er['count']), times + er['error']

        read_from_file.close()
        if exception_output_file:
            exception_output_file.close()
        if error_output_file:
            error_output_file.close()


def get_last_n_sql_statements(last_n_sql_stmnts, include_responses=True,
                              include_times=True, include_current_time=False):
    """Returns a (string) message, containing the last n SQL statements sent to
    sqlcmd; and, optionally, the last n responses received from sqlcmd; based on
    the current contents of last_n_sql_stmnts, where n is its size.
    """
    result = '\n'.join(sql['sql'] for sql in last_n_sql_stmnts) + '\n'
    if include_responses:
        result = '\n    sql statements (or other commands):\n' + result \
               + '\n    corresponding sqlcmd output:' \
               + ''.join(sql['output'] for sql in last_n_sql_stmnts)
        if include_times:
            result += '\n    at times: (sql sent; output received)\n' \
                    + '\n'.join(sql['sql-time']+'; '+sql['output-time'] for sql in last_n_sql_stmnts)
            if include_current_time:
                result += '\n' + formatted_time(time()) + ": timed out\n"
    return result


def print_summary(error_message=''):
    """Prints various summary messages, to STDOUT, and to the sqlcmd summary
    file (if specified and not STDOUT); also, closes those output files, as
    well as the SQL statement output file (i.e., sqlcmd input file), and the
    sqlcmd output file (if specified and not STDOUT). The summary messages may
    include: tails of the VoltDB server log file and/or of the VoltDB server's
    console (i.e., its STDOUT and STDERR); the total amount of execution time;
    the number and percent of valid and invalid SQL statements of various types,
    and their totals; and an error message, if one was specified, which usually
    means that execution was halted prematurely, due a VoltDB server crash. Any
    sqlcmd 'hangs' are also listed.
    """
    global start_time, sql_output_file, sqlcmd_output_file, sqlcmd_summary_file, \
        options, echo_substrings, count_sql_statements, last_n_sql_statements, \
        hanging_sql_commands, find_in_log_output_files

    # Generate the summary message (to be printed below)
    summary_message = ''
    try:
        last_sql_message = '\n\nLast ' + str(len(last_n_sql_statements)) + ' SQL statements sent to sqlcmd:\n' \
                         + get_last_n_sql_statements(last_n_sql_statements, False, False)
        seconds = time() - start_time
        summary_message  = '\n\nSUMMARY: in ' + re.sub('^0:', '', str(timedelta(0, round(seconds))), 1) \
                         + ' ({0:.3f} seconds)'.format(seconds) + ', SQL statements by type:'

        total_count = -1
        if count_sql_statements.get('total') and count_sql_statements.get('total').get('total'):
            total_count = count_sql_statements['total']['total']
        for sql_type in sorted(count_sql_statements):
            sql_type_count = -1
            if count_sql_statements[sql_type].get('total'):
                sql_type_count = count_sql_statements[sql_type]['total']
            # Make sure that 'valid' counts have matching 'invalid' counts, and vice versa
            if count_sql_statements[sql_type].get('valid') and not count_sql_statements[sql_type].get('invalid'):
                count_sql_statements[sql_type]['invalid'] = 0
            elif count_sql_statements[sql_type].get('invalid') and not count_sql_statements[sql_type].get('valid'):
                count_sql_statements[sql_type]['valid'] = 0
            summary_message += '\n    {0:6s}:'.format(sql_type)
            for validity in sorted(count_sql_statements[sql_type], reverse=True):
                if validity != 'total':  # save total for last
                    count = count_sql_statements[sql_type][validity]
                    percent = int(round(100.0 * count / sql_type_count))
                    summary_message += '{0:7d} '.format(count) + validity + ' ({0:3d}%),'.format(percent)
            percent = int(round(100.0 * sql_type_count / total_count))
            summary_message += '{0:8d} '.format(sql_type_count) + 'total ({0:3d}%)'.format(percent)
            if 'ECHO' in sql_type.upper():
                summary_message += "  - containing " + " AND ".join("'"+x+"'" for x in echo_substrings) + "]"
    except Exception as e:
        print '\n\nCaught exception attempting to print SUMMARY message:'
        print_exc()
        print '\n\nHere is count_sql_statements, which we were attempting to format & print:\n', count_sql_statements

    # Generate a message about sqlcmd 'hangs', if any (to be printed below)
    hanging_sql_message = ''
    try:
        if hanging_sql_commands:
            hanging_sql_message  = '\n\n\nFATAL ERROR: sqlcmd hanging due to the following set(s) of ' \
                                 + 'SQL statement(s) (or other commands) with unrecognized responses:\n' \
                                 + '\n'.join(sql for sql in hanging_sql_commands)
            summary_message += '\n\n\nFATAL ERROR: sqlcmd hanging due to the SQL statement(s) ' \
                             + '(or other commands) listed above, with unrecognized responses.'
    except Exception as e:
        print '\n\nCaught exception attempting to print HANGING sqlcmd message:'
        print_exc()
        print '\n\nHere is hanging_sql_commands, which we were attempting to format & print:\n', hanging_sql_commands

    # Print the summary messages, and close output file(s)
    if sql_output_file and sql_output_file is not sys.stdout:
        sql_output_file.close()
    if sqlcmd_output_file and sqlcmd_output_file is not sys.stdout:
        sqlcmd_output_file.close()
    for find in find_in_log_output_files:
        find['output_file'].close()
    if sqlcmd_summary_file and sqlcmd_summary_file is not sys.stdout:
        if options.log_files and options.log_number:
            sys.stdout.flush()
            for log_file in options.log_files.split(','):
                print_file_tail_and_errors(log_file, sqlcmd_summary_file, options.log_number)
        print >> sqlcmd_summary_file, hanging_sql_message, last_sql_message, summary_message, error_message
        sqlcmd_summary_file.close()
    print hanging_sql_message, last_sql_message, summary_message, error_message


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


def increment_sql_statement_type(type=None, validity=None, incrementTotal=True):
    """Increment the value of 'count_sql_statements' (a 2D dictionary, i.e.,
    a dict of dict), both for the 'total', 'total' element and for the 'type',
    if specified (i.e., for the type, 'total' element); also, if the 'validity'
    is specified (normally equal to 'valid' or 'invalid'), increment those
    values as well (i.e., the 'total', validity and type, validity elements).
    """

    if incrementTotal:
        increment_sql_statement_indexes('total', 'total')
        if validity:
            increment_sql_statement_indexes('total', validity)
    if type:
        increment_sql_statement_indexes(type, 'total')
        if validity:
            increment_sql_statement_indexes(type, validity)


class TimeoutException(Exception):
    """A custom exception class used for timeout when reading from
    the sqlcmd process.
    """
    pass


def timeout_handler(signum, frame):
    """A custom signal handler function, used for timeout when reading from
    the sqlcmd process.
    """
    raise TimeoutException


def formatted_time(seconds_since_epoch):
    """Takes a time representing the number of seconds since the beginning of
    the epoch and returns that time as a nicely formatted string.
    """
    return strftime('%Y-%m-%d %H:%M:%S', localtime(seconds_since_epoch)) + ' (' + str(seconds_since_epoch) + ')'


def print_sql_statement(sql, num_chars_in_sql_type=6):
    """Print the specified SQL statement (sql), to the SQL output file (which may
    be STDOUT); and, if the sqlcmd option was specified, pass that SQL statement
    to sqlcmd, and print its output in the sqlcmd output file (which may be
    STDOUT).
    """
    global sql_output_file, sqlcmd_output_file, echo_output_file, sqlcmd_proc, \
        last_n_sql_statements, options, echo_substrings, symbol_depth, symbol_order, \
        known_error_messages, known_valid_show_responses, hanging_sql_commands, \
        find_in_log_output_files, debug

    # Print the specified SQL statement to the specified output file
    print >> sql_output_file, sql

    # If an 'echo' substring was specified, and is found in the current SQL
    # statement, then echo it to the 'echo output file': this is useful for
    # debugging things that were recently added to the SQL grammar (e.g., a
    # new function name)
    sql_contains_echo_substring = False
    if options.echo and all(x in sql for x in echo_substrings):
        sql_contains_echo_substring = True
        print >> echo_output_file, '\n', sql, '\n'

    # If a sqlcmd sub-process has been defined, use it
    if sqlcmd_proc:

        # Save the last options.summary_number SQL statements, for the summary
        last_n_sql_statements.append({'sql':sql, 'sql-time':formatted_time(time()),
                                      'output':'', 'output-time':''})
        while len(last_n_sql_statements) > options.summary_number:
            last_n_sql_statements.pop(0)
        sqlLen = len(last_n_sql_statements)

        # Pass the SQL statement to the sqlcmd sub-process
        sqlcmd_proc.stdin.write(sql + '\n')
        sql_was_echoed_as_output = False

        # Kludge for certain 'exec @Statistics' commands, which return multiple
        # '(Returned N rows in X.XXs)' messages
        num_Returned_messages = 0
        expected_num_Returned_messages = 1  # normal case
        if '@Statistics' in sql:
            sql_upper = sql.upper()
            if 'EXEC' in sql_upper:
                if 'MANAGEMENT' in sql_upper:
                    expected_num_Returned_messages = 9
                elif 'DRCONSUMER' in sql_upper or 'DRPRODUCER' in sql_upper:
                    expected_num_Returned_messages = 2

        # Change the behavior of SIGALRM, to use for timeout
        signal(SIGALRM, timeout_handler)
        max_seconds_to_wait_for_sqlcmd = 60  # must be larger than query timeout of 10
        if debug > 4:
            print 'max_seconds_to_wait_for_sqlcmd: ' + str(max_seconds_to_wait_for_sqlcmd)

        output = None
        while True:

            # Retrieve (& print) the next line of output from the sqlcmd sub-process;
            # but if it takes too long, abort
            alarm(max_seconds_to_wait_for_sqlcmd)  # sets the alarm for the given # of seconds
            try:
                output = sqlcmd_proc.stdout.readline().rstrip('\n')
            except TimeoutException:
                hanging_sql_commands.append(get_last_n_sql_statements(last_n_sql_statements, include_current_time=True))
                if debug > 1:
                        print "\nERROR: timeout waiting for (hanging?) sqlcmd, after", \
                              str(max_seconds_to_wait_for_sqlcmd), "seconds, with:\n" + \
                              get_last_n_sql_statements(last_n_sql_statements, include_current_time=True)
                break
            else:
                alarm(0)  # turns off the alarm
            print >> sqlcmd_output_file, output
            last_n_sql_statements[sqlLen-1]['output'] += output + '\n'
            last_n_sql_statements[sqlLen-1]['output-time'] = formatted_time(time())

            # Debug print, if that 'echo' substring was found
            if sql_contains_echo_substring and sql_was_echoed_as_output:
                print >> echo_output_file, output

            if output:
                # Wait until the SQL statement has been echoed by sqlcmd,
                # before checking whether it was considered valid or invalid
                if output == sql:
                    sql_was_echoed_as_output = True
                # Kludge for ENG-13416
                elif '--' in sql and output == sql[:sql.index('--')]+';':
                    sql_was_echoed_as_output = True

                # Check if sqlcmd considered the SQL statement to be valid
                elif (('(Returned ' in output and ' rows in ' in output and 's)' in output) or
                        'Command succeeded.' in output):
                    num_Returned_messages += 1
                    if num_Returned_messages < expected_num_Returned_messages:
                        # Avoid double-counting certain 'exec @Statistics' commands,
                        # which can return multiple '(Returned N rows in X.XXs)' messages
                        continue
                    elif sql_was_echoed_as_output:
                        increment_sql_statement_type(sql[0:num_chars_in_sql_type], 'valid')
                        if sql_contains_echo_substring:
                            increment_sql_statement_type(' [echo', 'valid', False)
                        break
                    elif debug > 3:
                        # this can happen, though it's uncommon, when two SQL statements
                        # (DDL or otherwise) are issued together, e.g., DROP and CREATE
                        # PROCEDURE statements
                        print "\nDEBUG: Found '(Returned N rows in X.XXs)' or 'Command", \
                              "succeeded' before SQL echoed (rare condition), with:\n" + \
                              get_last_n_sql_statements(last_n_sql_statements)

                # Check if sqlcmd considered the SQL statement to be invalid,
                # indicated by the word 'ERROR' (case insensitive)
                elif 'ERROR' in output.upper():
                    if sql_was_echoed_as_output:
                        increment_sql_statement_type(sql[0:num_chars_in_sql_type], 'invalid')
                        if sql_contains_echo_substring:
                            increment_sql_statement_type(' [echo', 'invalid', False)
                        break
                    elif debug > 3:
                        # this can happen, though it's uncommon, when there is a multi-line
                        # error message, which uses the word 'ERROR' on more than one line
                        print "\nDEBUG: Found 'ERROR' before SQL echoed (rare condition), with:\n" + \
                              get_last_n_sql_statements(last_n_sql_statements)

                # Invalid 'exec', 'explainproc' & 'explainview' commands sometimes
                # respond with various messages that do not include 'ERROR'
                elif any( all(err_msg in output for err_msg in kem) for kem in known_error_messages):
                    if sql_was_echoed_as_output:
                        increment_sql_statement_type(sql[0:num_chars_in_sql_type], 'invalid')
                        if sql_contains_echo_substring:
                            increment_sql_statement_type(' [echo', 'invalid', False)
                        break
                    elif debug > 2:
                        # this can happen, though it's uncommon, when there is a multi-line
                        # error message, which uses the word 'ERROR' on one line and one of
                        # the known_error_messages, on another
                        print "\nDEBUG: Found invalid 'exec', 'explainproc', or 'explainview'", \
                              "error message before SQL echoed (rare condition), with:\n" + \
                              get_last_n_sql_statements(last_n_sql_statements)

                # Valid 'show' commands just return a list, with one of several valid headers;
                # also, for some reason, these commands don't get echoed back by sqlcmd, so we
                # don't check sql_was_echoed_as_output here
                elif any(kvsr in output for kvsr in known_valid_show_responses):
                    # Check for a special case: avoid double-counting 'show classes'
                    # commands, which can return anywhere from 1 to 3 lists of
                    # different types of classes (i.e., 'Potential Procedure Classes',
                    # 'Active Procedure Classes', 'Non-Procedure Classes')
                    if ('Procedure Classes' in output and sqlLen > 1
                            and not ('show' in sql and 'classes' in sql)):
                        previous_sql = last_n_sql_statements[sqlLen-2]['sql']
                        if 'show' in previous_sql and 'classes' in previous_sql:
                            continue

                    # Normal 'show' command case
                    increment_sql_statement_type(sql[0:num_chars_in_sql_type], 'valid')
                    if sql_contains_echo_substring:
                        increment_sql_statement_type(' [echo', 'valid', False)
                    break

                # Invalid 'show' commands return a simple error message; also, once again, these commands
                # don't get echoed back by sqlcmd, so we don't check sql_was_echoed_as_output here
                elif 'The valid SHOW command completions are' in output:
                    increment_sql_statement_type(sql[0:num_chars_in_sql_type], 'invalid')
                    if sql_contains_echo_substring:
                        increment_sql_statement_type(' [echo', 'invalid', False)
                    break

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

                # Check if the VoltDB server has been put into paused / read-only
                # mode, usually due using too much memory (RSS), typically caused
                # by a Recursive CTE causing an infinite loop
                elif ('Server is paused and is available in read-only mode' in output):
                    error_message = '\n\n\nFATAL ERROR: sqlcmd responded:\n    "' + output + \
                                    '"\npossibly due to a Recursive CTE (WITH) statement that caused ' + \
                                    'an infinite loop, consuming too much memory (RSS):\n    "' + sql + '"\n'
                    print_summary(error_message)
                    sqlcmd_proc.communicate('exit')
                    exit(98)

        for find in find_in_log_output_files:
            command = 'tail -n ' + str(options.find_number) + ' ' + find['log_file']
            if debug > 4:
                print 'DEBUG: tail command:', command
            tail_proc = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
            tail_of_log_file = tail_proc.communicate()[0].replace('\\n', '\n')
            for find_string in options.find_in_log.split(','):
                # Check if the string we're looking for is in the log file
                if find_string in tail_of_log_file:
                    # Make sure not to double-count something we found in the log file previously
                    prev = find['previous_tail']
                    if (prev not in tail_of_log_file or find_string in
                            tail_of_log_file[tail_of_log_file.index(prev)+len(prev):]):
                        print >> find['output_file'], 'Found "' + find_string + '" following these SQL statements:\n' \
                                + get_last_n_sql_statements(last_n_sql_statements, False, False) + '\n'
            find['previous_tail'] = tail_of_log_file[len(tail_of_log_file)/2:]

    else:
        increment_sql_statement_type(sql[0:num_chars_in_sql_type])

    if sql_contains_echo_substring and options.echo_grammar:
        print >> echo_output_file, '\nGrammar symbols used (in order), and how many times, and resulting SQL:'
        for (symbol, partial_sql, symbol_opts, existing_names, reuse_name, reuse_value) in symbol_order:
            print >> echo_output_file, "{0:1d}: {1:24s}: {2:s}".format(symbol_depth.get(symbol, 0), symbol, partial_sql)
            if symbol_opts:
                print >> echo_output_file, "   ;{0:23s}: {1:s}".format(symbol_opts, existing_names)
            if reuse_name:
                print >> echo_output_file, "   :{0:23s}: {1:s}".format(reuse_name, reuse_value)
        print >> echo_output_file, "{0:27s}: {1:s}".format('Final sql', sql)


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
            if debug > 3:
                print 'DEBUG: exceeded max_time, at:', formatted_time(time())
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
    parser.add_option("-r", "--seed", dest="seed", default=None,
                      help="seed for random number generator; a blank string, or None, means that the seed "
                          + "should itself be randomly generated [default: None]")
    parser.add_option("-i", "--initial_type", dest="initial_type", default="insert-statement",
                      help="a type, or comma-separated list of types, of SQL statements to generate initially; typically "
                          + "used to initialize the database using INSERT statements [default: insert-statement]")
    parser.add_option("-I", "--initial_number", dest="initial_number", default=5,
                      help="the number of each INITIAL_TYPE of SQL statement to generate [default: 5]")
    parser.add_option("-t", "--type", dest="type", default="sql-statement",
                      help="a type, or comma-separated list of types, of SQL statements to generate "
                         + "(after the initial ones, if any) [default: sql-statement]")
    parser.add_option("-n", "--number", dest="number", default=0,
                      help="the number of each TYPE of SQL statement to generate; a negative value "
                         + "means keep generating until the number of minutes is reached [default: 5; "
                         + "but -1 if MINUTES is nonzero]")
    parser.add_option("-m", "--minutes", dest="minutes", default=0,
                      help="the number of minutes to generate all SQL statements, of all types "
                         + "(if positive, overrides the NUMBER of SQL statements) [default: 0]")
    parser.add_option("-d", "--delete_type", dest="delete_type", default="truncate-statement",
                      help="a type of SQL statements used to delete data periodically, so that a VoltDB "
                         + "server's memory does not grow too large [default: truncate-statement]")
    parser.add_option("-T", "--delete_number", dest="delete_number", default=10,
                      help="the number of DELETE_TYPE SQL statements to generate, each time [default: 10]")
    parser.add_option("-x", "--max_save", dest="max_save", default=1000,
                      help="the maximum number of SQL statements (and their results, if sqlcmd is called) to save "
                         + "in the output files; after this many SQL statements, the output files are erased, and "
                         + "DELETE_TYPE statements are called, to clear the database and start fresh [default: 1000]")
    parser.add_option("-o", "--output", dest="sql_output", default="sqlcmd.in",
                      help="an output file path/name, to which to send all generated SQL statements; "
                         + "if not specified, output goes to STDOUT [default: sqlcmd.in]")
    parser.add_option("-O", "--sqlcmd", dest="sqlcmd_output", default="sqlcmd.out",
                      help="an output file path/name, to which sqlcmd output is sent, or STDOUT to send the output there; the "
                         + "generated SQL statements are only passed to sqlcmd if this value exists [default: sqlcmd.out]")
    parser.add_option("-s", "--summary", dest="sqlcmd_summary", default="summary.out",
                      help="an output file path/name, to which a summary of all sqlcmd output is sent; a brief "
                         + "summary also goes to STDOUT, assuming that 'sqlcmd' exists [default: summary.out]")
    parser.add_option("-S", "--summary_number", dest="summary_number", default=5,
                      help="the number of SQL statements (the last ones sent to sqlcmd) to output to the 'summary' "
                         + "file; only applies if 'sqlcmd' and 'summary' are also specified [default: 5]")
    parser.add_option("-l", "--log", dest="log_files", default=None,
                      help="a file path/name, or comma-separated list of file paths/names, such as the VoltDB log "
                         + "file or console output; if this and SQLCMD_SUMMARY are specified, the summary will "
                         + "include a 'tail' of each of these files [default: None]")
    parser.add_option("-L", "--log_number", dest="log_number", default=100,
                      help="the number of lines to 'tail' from the 'log' file(s); only applies "
                         + "if SQLCMD_SUMMARY and LOG_FILES are also specified [default: 100]")
    parser.add_option("-f", "--find_in_log", dest="find_in_log", default=None,
                      help="a substring, or comma-separated list of substrings, to be searched for in the 'log' "
                         + "file(s); if this is specified, then an attempt will be made to search the FIND_FILES "
                         + "(or LOG_FILES) for these substring(s), and determine which SQL statement (or other "
                         + "sqlcmd command) caused the substring to appear in the log file; useful for finding "
                         + "examples of SQL statements that cause specific Java Exceptions (e.g. NullPointerException), "
                         + "or other errors, to appear in the log or console; any output from such searches will "
                         + "appear in a file using the name of the log file, preceded by 'found_in_'; note that "
                         + "this is slow, so should not be used routinely, only when tracking down specific issues "
                         + "[default: None]")
    parser.add_option("-F", "--find_files", dest="find_files", default=None,
                      help="a file path/name, or comma-separated list of file paths/names, such as the VoltDB log "
                         + "file or console output: the file(s) in which to search for the FIND_IN_LOG substring(s), "
                         + "if specified [default: LOG_FILES]")
    parser.add_option("-N", "--find_number", dest="find_number", default=100,
                      help="the number of lines to 'tail' from the FIND_FILES (or LOG_FILES); only applies "
                         + "if FIND_IN_LOG is also specified [default: 100]")
    parser.add_option("-e", "--echo", dest="echo", default=None,
                      help="a substring, or comma-separated list of substrings, to be searched for in all SQL "
                         + "statements sent to sqlcmd: if this is specified, then all SQL statements that contain "
                         + "this substring (or list of substrings), and their results, will be echoed (to "
                         + "ECHO_FILE); this is useful for debugging new features, e.g., if you just added the "
                         + "LOG10 function, you can set this to 'LOG10', to see its effect [default: None]")
    parser.add_option("-E", "--echo_file", dest="echo_file", default="sqlcmd.echo",
                      help="a file path/name to which to send ECHO output; if not specified, ECHO output "
                         + "(if any) goes to STDOUT [default: sqlcmd.echo]")
    parser.add_option("-G", "--echo_grammar", dest="echo_grammar", default=False,
                      help="a boolean value (True or False), specifying whether to include, in the ECHO_FILE, "
                         + "the list of grammar symbols, and how many times each one was used, for each of the "
                         + "SQL statements that is echoed [default: False]")
    parser.add_option("-X", "--suffix", dest="suffix", default=None,
                      help="a suffix to be appended to the various output file names (but before their extensions); "
                         + "e.g., if SQLCMD_OUTPUT has its default value of 'sqlcmd.out', and SUFFIX is '123', then "
                         + "the actual sqlcmd output file will be named 'sqlcmd123.out' [default: None]")
    parser.add_option("-D", "--debug", dest="debug", default=2,
                      help="print debug info: 0 for none, increasing values (1-8) for more [default: 2]")
    (options, args) = parser.parse_args()

    # The default 'number' depends on whether 'minutes' is zero or not
    if not options.number:
        if options.minutes:
            options.number = -1
        else:
            options.number = 5

    debug = int(options.debug)
    if debug > 1:
        print "DEBUG: all arguments:", " ".join(sys.argv)
        print "DEBUG: options (all):\n", options
        print "DEBUG: args (all)            :", args
        print "DEBUG: options.path          :", options.path
        print "DEBUG: options.grammar_files :", options.grammar_files
        print "DEBUG: options.seed          :", options.seed
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
        print "DEBUG: options.find_in_log   :", options.find_in_log
        print "DEBUG: options.find_files    :", options.find_files
        print "DEBUG: options.find_number   :", options.find_number
        print "DEBUG: options.echo          :", options.echo
        print "DEBUG: options.echo_file     :", options.echo_file
        print "DEBUG: options.echo_grammar  :", options.echo_grammar
        print "DEBUG: options.suffix        :", options.suffix
        print "DEBUG: options.debug         :", options.debug

    if options.seed:
        seed_type   = 'supplied'
        random_seed = int(options.seed)
    else:
        seed_type   = 'random'
        random_seed = randrange(2 ** 63)
    print 'Using %s seed: %d' % (seed_type, random_seed)
    seed(random_seed)

    # If a maximum number of minutes was specified, compute the time at which
    # to stop execution
    start_time = time()
    max_time = 0
    if options.minutes:
        max_time = start_time + 60*int(options.minutes)
    if debug > 1:
        print 'DEBUG: start time:', formatted_time(start_time)
        print 'DEBUG: max_time  :', formatted_time(max_time)
        sys.stdout.flush()

    # Define the grammar to be used to generate SQL statements, based upon
    # the input grammar file(s)
    grammar = {}
    for grammar_file in options.grammar_files.split(','):
        grammar = get_grammar(grammar, grammar_file, options.path)
    symbol_depth = {}
    symbol_order = []

    if debug > 7:
        print 'DEBUG: grammar:'
        for key in grammar.keys():
            print '   ', key + ':', grammar[key]
        print

    # Open the output file for generated SQL statements, if specified
    sql_output_file = sys.stdout
    if options.sql_output:
        sql_output_file = open(add_suffix_to_file_name(options.sql_output, options.suffix), 'w', 0)

    # Open the output file for SQL statements to be echoed, if specified
    echo_output_file = None
    echo_substrings = []
    if options.echo:
        echo_substrings = options.echo.split(",")
        echo_output_file = sys.stdout
        if options.echo_file:
            echo_output_file = open(add_suffix_to_file_name(options.echo_file, options.suffix), 'w', 0)
            print >> echo_output_file, "SQL statements containing " \
                + " AND ".join("'"+x+"'" for x in echo_substrings) + ":\n"

    # Open the sub-process used to execute SQL statements in sqlcmd,
    # and the output file for sqlcmd results, if specified
    sqlcmd_proc = None
    sqlcmd_output_file = None
    sqlcmd_summary_file = None
    last_n_sql_statements = []
    find_in_log_output_files = []
    if options.sqlcmd_output:
        if options.sqlcmd_output is "STDOUT":
            sqlcmd_output_file = sys.stdout
        else:
            sqlcmd_output_file = open(add_suffix_to_file_name(options.sqlcmd_output, options.suffix), 'w', 0)

        if options.sqlcmd_summary:
            sqlcmd_summary_file = open(add_suffix_to_file_name(options.sqlcmd_summary, options.suffix), 'w', 0)
            print >> sqlcmd_summary_file, 'Using %s seed: %d' % (seed_type, random_seed)

        if options.find_in_log:
            files = options.log_files  # default if options.find_files not specified
            if options.find_files:
                files = options.find_files
            for lf in files.split(','):
                log_file_name = lf[lf.rfind('/') + 1:]
                output_file = open(add_suffix_to_file_name('found_in_'+log_file_name, options.suffix), 'w', 0)
                find_in_log_output_files.append({'log_file':lf, 'output_file':output_file,
                                                 'previous_tail':'INITIAL_VALUE_PRESUMABLY_NOT_IN_LOG_FILE'})

        command = 'sqlcmd --stop-on-error=false'
        if debug > 4:
            print 'DEBUG: sqlcmd command:', command
        sqlcmd_proc = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)

    # A list of known error messages (besides the case-insensitive word 'ERROR'),
    # which indicate that sqlcmd considers the SQL statement (or other command)
    # given to it to be invalid. Note that these are lists so that the error
    # messages can be split into two (or more) parts when needed; e.g.,
    # 'Procedure FOO not in catalog' is split into 'Procedure' and
    # 'not in catalog', both of which must appear in the error message, but with
    # an unknown procedure name in between.
    known_error_messages = [['Undefined procedure'],
                            ['Invalid parameter count for procedure'],
                            ['Invalid parameter:  Expected'],
                            ['Invalid', 'selector'],
                            ['tude', 'out of bounds'],  # Longitude or Latitude
                            ['tude out of range in GeographyPointValue constructor'],
                            ['No such file or directory'],
                            ['must be a valid', 'selector'],
                            ['Unable to update deployment configuration'],
                            ['is not a supported type of partition key'],
                            ['Procedure', 'not in catalog'],
                            ['View', 'does not exist'],
                            ["Explain doesn't support DDL"],
                            ['PartitionInfo specifies invalid parameter index for procedure'],
                            ['Failed to plan for statement'],
                            ['Invalid parameter index value']
                           ]

    # A list of headers found in responses to valid 'show' commands: one of
    # these is always found, in response to a valid 'show' command
    known_valid_show_responses = ['--- User Export Streams ---',
                                  '--- System Procedures ---',
                                  '--- User-defined Functions ---',
                                  '--- Empty Class List ---',
                                  'Procedure Classes ---']

    # Initialize a list of any SQL (or other) commands that may hang sqlcmd
    hanging_sql_commands = []

    # Generate the specified number of each type of SQL statement;
    # and run each in sqlcmd, if the sqlcmd option was specified
    count_sql_statements = {}
    if options.initial_number:
        for sql_statement_type in options.initial_type.split(','):
            generate_sql_statements(sql_statement_type, int(options.initial_number))
    for sql_statement_type in options.type.split(','):
        generate_sql_statements(sql_statement_type, int(options.number), int(options.max_save),
                                options.delete_type, options.delete_number)

    if debug > 5:
        print_sql_statement('select * from P1;')
        print_sql_statement('select * from R1;')
        print_sql_statement('select ID, TINY, SMALL, INT, BIG, NUM, DEC, VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, TIME from P1;')
        print_sql_statement('select ID, TINY, SMALL, INT, BIG, NUM, DEC, VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, TIME from R1;')
        print_sql_statement('select count(ID), count(TINY), count(SMALL), count(INT), count(BIG), count(NUM), count(DEC), count(VCHAR), count(VCHAR_INLINE_MAX), count(VCHAR_INLINE), count(TIME), count(VARBIN), count(POINT), count(POLYGON) from P1;')
        print_sql_statement('select count(ID), count(TINY), count(SMALL), count(INT), count(BIG), count(NUM), count(DEC), count(VCHAR), count(VCHAR_INLINE_MAX), count(VCHAR_INLINE), count(TIME), count(VARBIN), count(POINT), count(POLYGON) from R1;')

    if debug > 1:
        print 'DEBUG: end time  :', formatted_time(time())

    print_summary()

    # Exit sqlcmd, if it was opened
    if sqlcmd_proc:
        sqlcmd_proc.communicate('exit')

    if hanging_sql_commands:
        exit(97)
