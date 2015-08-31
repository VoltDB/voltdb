grammar SQLParser;


@header {
package org.voltdb.sqlparser.syntax.grammar;
}

data_query_list:
        dql_statement ( ';' dql_statement )*  EOF
    ;
    
data_definition_list: 
        ddl_statement ( ';' ddl_statement )* EOF
    ;

data_manipulation_list:
        dml_statement ( ';' dml_statement )* EOF
    ;
    
dql_statement:
        query_specification
    ;
    
query_specification:
        SELECT ( set_quantifier )? select_list table_expression
    ;

set_quantifier:
        DISTINCT
    |
        ALL
    ;
    
select_list:
       ASTERISK
    |
        select_sublist  (',' select_sublist)*
    ;
    
select_sublist:
        derived_column
    |
        qualified_asterisk
    ;

derived_column:
        value_expression ( as_clause )? 
    ;
    
as_clause:
        AS column_name
    ;

qualified_asterisk:
        asterisked_identifier_chain DOT ASTERISK
    ;

asterisked_identifier_chain:
        asterisked_identifier ( DOT asterisked_identifier )*
    ;

asterisked_identifier:
        IDENTIFIER
    ;
        
table_expression:
         from_clause
         ( where_clause )?
         ( group_by_clause )?
         ( having_clause )?
         ( window_clause )?
     ;
         
from_clause:
        FROM table_reference ( ',' table_reference )
    ;

table_reference:
        table_primary_or_joined_table
    ;
    
table_primary_or_joined_table:
        table_primary
    |
        joined_table
    ;   

table_primary:
        table_or_query_name ( ( AS )? correlation_name ( '(' derived_column_list ')' ) )?
     |     derived_table ( AS )* correlation_name ( '(' derived_column_list ')' )*
     |     '(' joined_table ')'
     ;

table_or_query_name:
        table_name
    |
        query_name
    ;

derived_column_list:
        column_name ( ',' column_name ) *
    ;

derived_table:
        table_subquery
    ;

table_subquery:
        subquery
    ;

/*************************************************************************
 * Expressions.
 *************************************************************************/
value_expression:
        common_value_expression
    |
        boolean_value_expression
    |
        row_value_expression
    ;
        
common_value_expression:
        numeric_value_expression
    |
        string_value_expression
    |
        datetime_value_expression
    |
        interval_value_expression
    |
        reference_value_expression
    ;

numeric_value_expression:
        <assoc=right> numeric_value_expression '*' numeric_value_expression
    |
        <assoc=right> numeric_value_expression '/' numeric_value_expression
    |
        <assoc=right>numeric_value_expression '+' numeric_value_expression
    |
        <assoc=right> numeric_value_expression '-' numeric_value_expression
    |
        term
    ;

term:
        sign numeric_primary
    ;

numeric_primary:
        value_expression_primary
    |
        numeric_value_function
    ;

numeric_value_function:
        position_expression
    |
        extract_expression
    |
        length_expression
    |
        absolute_value_expression
    |
        modulus_expression
    |
        natural_logarithm
    |
        exponential_function
    |
        power_function
    |
        square_root
    |
        floor_function
    |
        ceiling_function
    ;

position_expression:
        string_position_expression
    |
        blob_position_expression
    ;

string_position_expression:
        POSITION '(' string_value_expression IN string_value_expression [ USING char_length_units ] ')'
    ;

blob_position_expression:
        POSITION '(' blob_value_expression IN blob_value_expression ')'
    ;

length_expression:
        char_length_expression
    |
        octet_length_expression
    ;

char_length_expression:
         ( CHAR_LENGTH | CHARACTER_LENGTH ) '(' string_value_expression ( USING char_length_units )? ')'

octet_length_expression:
        OCTET_LENGTH '(' string_value_expression ')'
    ;

extract_expression:
        EXTRACT '(' extract_field FROM extract_source ')'
    ;

extract_field:
        primary_datetime_field | time_zone_field
    ;

time_zone_field:
        TIMEZONE_HOUR
    |
        TIMEZONE_MINUTE
    ;

extract_source:
        datetime_value_expression
    |
        interval_value_expression
    ;

absolute_value_expression:
        ABS '(' numeric_value_expression ')'
    ;

modulus_expression:
        MOD '(' numeric_value_expression_dividend ',' numeric_value_expression_divisor ')'
    ;

natural_logarithm:
        LN '(' numeric_value_expression ')'
    ;

exponential_function:
        EXP '(' numeric_value_expression ')'
    ;

power_function:
        POWER '(' numeric_value_expression_base ',' numeric_value_expression_exponent ')'
    ;

numeric_value_expression_base:
        numeric_value_expression
    ;

numeric_value_expression_exponent:
        numeric_value_expression
    ;

square_root:
        SQRT '(' numeric_value_expression ')'
    ;

floor_function:
        FLOOR '(' numeric_value_expression ')'
    ;

ceiling_function:
        { CEIL | CEILING } '(' numeric_value_expression ')'
    ;

string_value_expression:
        character_value_expression
    |
        blob_value_expression
    ;

character_value_expression:
        concatenation
    |
        character_factor
    ;

concatenation:
        character_value_expression concatenation_operator character_factor
    ;

character_factor:
        character_primary
    ;

character_primary:
        value_expression_primary
    |
        string_value_function
    ;

blob_value_expression:
        blob_concatenation
    |
        blob_factor
    ;

blob_factor:
        blob_primary
    ;

blob_primary:
        value_expression_primary
    |
        string_value_function
    ;

blob_concatenation:
        blob_value_expression concatenation_operator blob_factor
    ;

datetime_value_expression:
        datetime_term
    |
        interval_value_expression plus_sign datetime_term
    |
        datetime_value_expression plus_sign interval_term
    |
        datetime_value_expression minus_sign interval_term

datetime_term:
        datetime_factor
    ;

datetime_factor:
        datetime_primary [ time_zone ]
    ;

datetime_primary:
        value_expression_primary
    |
        datetime_value_function
    ;

time_zone:
        AT time_zone_specifier
    ;

time_zone_specifier:
        LOCAL
    |
        TIME ZONE interval_primary
    ;

datetime_value_function:
        current_date_value_function
    |
        current_time_value_function
    |
        current_timestamp_value_function
    |
        current_local_time_value_function
    |
        current_local_timestamp_value_function
    ;

current_date_value_function:
        CURRENT_DATE

current_time_value_function:
        CURRENT_TIME ( '(' time_precision ')' )?
    ;

current_local_time_value_function:
        LOCALTIME ( '(' time_precision ')' )?
    ;

current_timestamp_value_function:
        CURRENT_TIMESTAMP ( '(' timestamp_precision ')' )?
    ;

current_local_timestamp_value_function:
        LOCALTIMESTAMP ( '(' timestamp_precision ')' )?
    ;



interval_value_expression:
        interval_term
    |
        interval_value_expression_1 plus_sign interval_term_1
    |
        interval_value_expression_1 minus_sign interval_term_1
    |
        left_paren datetime_value_expression minus_sign datetime_term right_paren interval_qualifier
    ;

interval_term:
        interval_factor
    |
        interval_term_ asterisk factor
    |
        interval_term_ solidus factor
    |
        term asterisk interval_factor
    ;

interval_factor:
        [ sign ] interval_primary
    ;

interval_primary:
         value_expression_primary ( interval_qualifier )?
     |
         interval_value_function
     ;

interval_value_expression_1:
        interval_value_expression
    ;

interval_term_1:
        interval_term
    ;

interval_term_2:
        interval_term
    ;

value_expression_primary:
        '(' value_expression ')'
    |
        nonparenthesized_value_expression
    ;

time_precision:
        time_fractional_seconds_precision
    ;

timestamp_precision:
        time_fractional_seconds_precision
    ;

time_fractional_seconds_precision:
        unsigned_integer
    ;

nonparenthesized_value_expression:
    |
        unsigned_value_specification
    |
        column_reference
    |
        window_function
    |
        scalar_subquery
    |
        case_expression
    |
        cast_specification
    |
        field_reference
    |
        subtype_treatment
    |
        method_invocation
    |
        static_method_invocation
    |
        new_specification
    |
        attribute_or_method_reference
    |
        reference_resolution
    |
        collection_value_constructor
    |
        array_element_reference
    |
        multiset_element_reference
    |
        routine_invocation
    |
        next_value_expression
    ;

column_reference:
        IDENTIFIER ( DOT IDENTIFIER ) *
    |
        MODULE DOT qualified_identifier DOT column_name
    ;

set_function_specification:
        aggregate_function
    |
        grouping_operation
    ;

aggregate_function:
        COUNT '(' ASTERISK ')' [ filter_clause ]
    |
        general_set_function [ filter_clause ]
    |
        binary_set_function [ filter_clause ]
    |
        ordered_set_function [ filter_clause ] 
    ;

general_set_function:
        set_function_type '(' [ set_quantifier ] value_expression ')'
    ;

set_function_type:
        computational_operation
    ;
computational_operation:
        AVG
    |
        MAX
    |
        MIN
    |
        SUM
    |
        EVERY
    |
        ANY
    |
        SOME
    |
        COUNT
    |
        STDDEV_POP
    |
        STDDEV_SAMP
    |
        VAR_SAMP
    |
        VAR_POP
    |
        COLLECT
    |
        FUSION
    |
        INTERSECTION         
    ;                                                            

binary_set_function:
        binary_set_function_type '(' dependent_variable_expression ',' independent_variable_expression ')'
    ;

binary_set_function_type:
        COVAR_POP
    |
        COVAR_SAMP
    |
        CORR
    |
        REGR_SLOPE
    |
        REGR_INTERCEPT
    |
        REGR_COUNT
    |
        REGR_R2
    |
        REGR_AVGX
    |
        REGR_AVGY
    |
        REGR_SXX
    |
        REGR_SYY
    |
        REGR_SXY         
    ;

filter_clause:
        FILTER '(' WHERE search_condition ')'
    ;

dependent_variable_expression:
        numeric_value_expression
    ;

independent_variable_expression:
        numeric_value_expression
    ;

ordered_set_function:
        hypothetical_set_function
    |
        inverse_distribution_function
    ;

hypothetical_set_function:
        rank_function_type '(' hypothetical_set_function_value_expression_list ')' within_group_specification
    ;

rank_function_type:
        RANK
    |
        DENSE_RANK
    |
        PERCENT_RANK
    |
        CUME_DIST
    ;

hypothetical_set_function_value_expression_list:
        value_expression ( ',' value_expression )
    ;

within_group_specification:
        WITHIN GROUP '(' ORDER BY sort_specification_list ')'
    ;


sort_specification_list:
        sort_specification ( ( ',' sort_specification )? )*
    ;

sort_specification:
        sort_key ( ordering_specification )* ( null_ordering )*
    ;

sort_key:
        value_expression
    ;

ordering_specification:
        ASC
    |
        DESC
    ;

null_ordering:
        NULLS FIRST
    |
        NULLS LAST
    ;

window_function:
        window_function_type OVER window_name_or_specification
    ;

window_function_type:
        rank_function_type '(' ')'
   |
        ROW_NUMBER '(' ')'
   |
        aggregate_function
   ;

window_name_or_specification:
        window_name
    |
        in_line_window_specification
    ;

in-line_window_specification:
        window_specification
    ;

grouping_operation:
        GROUPING '(' column_reference ( ( ',' column_reference )? )? ')'
    ;

window_function:
    ;

scalar_subquery:
    ;

case_expression:
    ;

cast_specification:
    ;

field_reference:
    ;

subtype_treatment:
    ;

method_invocation:
    ;

static_method_invocation:
    ;

new_specification:
    ;

attribute_or_method_reference:
    ;

reference_resolution:
    ;

collection_value_constructor:
    ;

array_element_reference:
    ;

multiset_element_reference:
    ;

routine_invocation:
    ;

next_value_expression:
    ;


unsigned_value_specification:
        unsigned_literal
    |
        general_value_specification
    ;

general_value_specification:
        CURRENT_USER
    |
        SESSION_USER
    |
        SYSTEM_USER
    |
        USER
    |
        VALUE 
    ;

subquery:
        '(' query_expression ')'
    ;

where_clause:
        WHERE search_condition
    ;

expression:
                '(' expression ')'                #null_expr
        |
                expression op=timesop expression  #times_expr
        |
                expression op=addop expression    #add_expr
        |
                expression op=relop expression    #rel_expr
        |
                NOT expression                    #not_expr
        |
                expression AND expression          #conjunction_expr
        |
                expression OR expression          #disjunction_expr
        |
                boolconst=TRUE                    #true_expr
        |
                boolconst=FALSE                   #false_expr
        |
                col=column_ref                    #colref_expr
        |
                num=NUMBER                        #numeric_expr
        ;
        
timesop:
                '*'|'/'
        ;
        
addop:
                '+'|'-'
        ;
        
relop:
                '='|'<'|'>'|'<='|'>='|'!='
        ;

ddl_statement:
    |
        create_table_statement
    ;
    
dml_statement:
        insert_statement
    ;

create_table_statement:
        CREATE TABLE table_name '(' column_definitions ')'
    ;

column_definitions:
        column_definition (',' column_definition )*
    ; 

column_definition:
        column_name type_expression  ( column_attribute )*
    ;

column_attribute:
        ( NOT )? NULL           #nullableColumnAttribute
    |
        DEFAULT literal_value   #defaultValueColumnAttribute
    |
        PRIMARY KEY             #primaryKeyColumnAttribute
    |
        UNIQUE                  #uniqueColumnAttribute
    ;

literal_value:
        NUMBER
    |
        STRING_LITERAL
    ;
    
type_expression:
        type_name ( '(' NUMBER ( ',' NUMBER )* ')' )?
    ;

insert_statement:
        INSERT INTO table_name ( column_name_list )?
        VALUES values
    ;
        
column_name_list:
        '(' column_name ( ',' column_name )* ')'
    ;

values: '(' value ( ',' value )* ')';

value:
        NUMBER
    ;        

/* Names */
type_name: IDENTIFIER ;

table_name: IDENTIFIER ;

column_name: IDENTIFIER ;

alias_name: IDENTIFIER;

query_name: IDENTIFIER;

correlation_name: IDENTIFIER;

qualified_identifier: IDENTIFIER;

unsigned_integer: NUMBER;

signed_integer: ( '-' | '+' )? NUMBER;

NUMBER: ( NZDIGIT ( DIGIT )* ) | ZERODIGIT;

/*
 * Note: I think this is wrong.  I think double quotes may
 *       work somehow.
 */
SQ_STRING_LITERAL: '\'' (~('\'' | '\r' | '\n') | ('\'' '\'') )* '\'';
DQ_STRING_LITERAL: '"'  (~('\"' | '\r' | '\n') | ('\"' '\"') )* '\'';
STRING_LITERAL: SQ_STRING_LITERAL | DQ_STRING_LITERAL;
CREATE:      C R E A T E;
TABLE:       T A B L E;
INSERT:      I N S E R T;
INTO:        I N T O;
VALUES:      V A L U E S;
SELECT:      S E L E C T;
FROM:        F R O M;
WHERE:       W H E R E;
AS:          A S;
TRUE:        T R U E;
FALSE:       F A L S E;
NOT:         N O T;
NULL:        N U L L;
AND:         A N D;
OR:          O R;
DEFAULT:     D E F A U L T;
VALUE:       V A L U E;
PRIMARY:     P R I M A R Y;
KEY:         K E Y;
UNIQUE:      U N I Q U E;
ASTERISK:    '*';
DISTINCT:    D I S T I N C T;
ALL:         A L L;
DOT:         '.';

IDENTIFIER: LETTER ( LETTER | DIGIT )*;

fragment
LETTER: [a-zA-Z\u0080-\u00FF_] ;

fragment
NZDIGIT: [1-9];

fragment
ZERODIGIT: '0';

fragment
DIGIT: [0-9];

fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');

/* Comments */
COMMENT: '#' .*? ('\r')? '\n' -> skip;
SPACE: [ \t\n] -> skip;
