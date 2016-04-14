
import sys
import os.path
import re
from random import randrange

__SYMBOL_DEFN  = re.compile(r"(?P<symbolname>[\w-]+)\s*::=\s*(?P<definition>.+)")
__SYMBOL_REF   = re.compile(r"{(?P<symbolname>[\w-]+)}")
__OPTIONAL     = re.compile(r"\[(?P<optionaltext>[^\[\]]*)\]")
__WEIGHTED_XOR = re.compile(r"\s+(?P<weight>\d*)(?P<xor>\|)\s+")
__XOR         = ' | '
USE_WEIGHTED_XOR = False


def get_grammar(grammar={}, grammar_filename='sql-grammar.txt', grammar_dir='.'):
    """Reads the SQL grammar from the specified file, in the specified directory.
    """

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
            print 'Unrecognized grammar rule:', grammar_rule
            continue
        symbol_name = grammar_defn.group('symbolname').strip()
        definition  = grammar_defn.group('definition').strip()

        weighted_xor = re.search(__WEIGHTED_XOR, definition)
        print 'DEBUG: __WEIGHTED_XOR:', str(__WEIGHTED_XOR)
        print 'DEBUG: definition:', definition
        print 'DEBUG: weighted_xor :', str(weighted_xor)
        weights = []
        for wxor in __WEIGHTED_XOR.finditer(definition):
            print 'DEBUG: weight:', wxor.group('weight')
            print 'DEBUG: xor   :', wxor.group('xor')
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

    print 'DEBUG: grammar:'
    for key in grammar.keys():
        print '   ', key + ':', grammar[key]
    print
    return grammar


def get_one_sql_statement(sql_statement_type='select-statement', max_depth=5, optional_percent=50):
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


def print_sql_statement(sql):
    """TBD
    """
    print sql



if __name__ == "__main__":
    # print the whole command line, maybe useful for debugging
#     print " ".join(sys.argv)

    #TODO: temp debug:
    USE_WEIGHTED_XOR = True
    grammar = get_grammar({}, 'temp-test-grammar.txt')
    for i in range(20):
        sql = get_one_sql_statement('int-column-name')
        print_sql_statement(sql)
    exit()

    grammar = get_grammar()
    grammar = get_grammar(grammar, 'sqlcov-modified-grammar.txt')
 
    for i in range(10):
        sql = get_one_sql_statement('insert-values-statement')
        print_sql_statement(sql)
 
    for i in range(10):
        sql = get_one_sql_statement('insert-statement')
        print_sql_statement(sql)
 
    for i in range(10):
        sql = get_one_sql_statement('upsert-statement')
        print_sql_statement(sql)
 
    for i in range(10):
        sql = get_one_sql_statement()
        print_sql_statement(sql)

    # TODO: temp. debug:
#     print 'select ID, TINY, SMALL, INT, BIG, NUM, DEC, VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, TIME, VARBIN from P1;'
#     print 'select ID, TINY, SMALL, INT, BIG, NUM, DEC, VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, TIME, VARBIN from R1;'
    print 'select ID, TINY, SMALL, INT, BIG, NUM, DEC, VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, TIME from P1;'
    print 'select ID, TINY, SMALL, INT, BIG, NUM, DEC, VCHAR, VCHAR_INLINE_MAX, VCHAR_INLINE, TIME from R1;'
