grammar SQLParser;


@header {
package org.voltdb.sqlparser.syntax.grammar;
}

/*
 * This is a sequence of any kind of statement.  It is
 * to be used in something like sqlcmd, which does not
 * distinguish between ddl, dml and dql statements.
 *
 * VOLT, INTERFACE_SYMBOL
 */
sql_statement_list:
        ddl_statement_list
    |
        dql_statement_list
    ;

/*
 * This is used in contexts where only dql statements are wanted,
 * such as a stored procedure.
 *
 * VOLT, INTERFACE_SYMBOL
 */
dql_statement_list:              // VOLT, INTERFACE_SYMBOL
        ( dql_statement (';' dql_statement )* ) EOF
    ;

/*
 * This is used in contexts where only ddl statements are wanted.
 */
ddl_statement_list:              // VOLT, INTERFACE_SYMBOL
        ( ddl_statement (';' ddl_statement )* ) EOF
    ;

dql_statement:
        query_specification
    |
        insert_or_upsert_statement
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
        table_name 
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
     ;
         
from_clause:
        FROM table_reference ( ',' table_reference )
    ;

table_reference:
        table_primary_or_joined_table ( sample_clause )*
    ;
    
table_primary_or_joined_table:
        table_primary
    |
        joined_table
    ;   

table_primary:
        table_or_query_name ( ( AS )? correlation_name ( '(' derived_column_list ')' )* )*
     |     derived_table ( AS )* correlation_name ( '(' derived_column_list ')' )*
     |     lateral_derived_table ( AS )* correlation_name ( '(' derived_column_list ')' )*
     |     collection_derived_table ( AS )* correlation_name ( '(' derived_column_list ')' )*
     |     table_function_derived_table ( AS )* correlation_name ( '(' derived_column_list ')' )*
     |     only_spec ( ( AS )* correlation_name ( '(' derived_column_list ')' )* )*
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

lateral_derived_table:
        LATERAL table_subquery
    ;

collection_derived_table:
        UNNEST '(' collection_value_expression ')' ( WITH ORDINALITY )*
    ;

table_function_derived_table:
        TABLE '(' collection_value_expression ')'
    ;

only_spec:
        ONLY '(' table_or_query_name ')'
    ;

collection_value_expression:
        array_value_expression
    |
        multiset_value_expression
    ;

array_value_expression:
        array_factor ( array_concatenation_operator array_factor )*
    ;

array_concatenation_operator:
        BARBARA
    ;

array_factor:
        value_primary_expression
    ;

value_primary_expression:
        '(' value_expression ')'
    |
        nonparenthesized_value_expression
    ;

nonparenthesized_value_expression:
    |
        unsigned_value_specification
    |
        column_reference
    |
        set_function_specification
    |
        window_function %%%
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
        host_parameter_specification
    |
        SQL_parameter_reference
    |
        dynamic_parameter_specification
    |
        embedded_variable_specification
    |
        current_collation_specification
    |
        CURRENT_DEFAULT_TRANSFORM_GROUP
    |
        CURRENT_PATH
    |
        CURRENT_ROLE
    |
        CURRENT_TRANSFORM_GROUP_FOR_TYPE path_resolved_user_defined_type_name
    |
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

sample_clause:
         TABLESAMPLE sample_ method '(' sample_percentage ')' ( repeatable_clause )*
    ;


sample_method:
        BERNOULLI
    |
        SYSTEM
    ;

sample_percentage:
        numeric_value_expression
    ;
    
repeatable_clause:
        REPEATABLE '(' repeat_argument ')'
    ;

repeat_argument:
        numeric_value_expression
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
        sql_schema_statement
    ;

sql_schema_statement:
        sql_schema_definition_statement
    |
        sql_schema_manipulation_statement
    ;

sql_schema_definition_statement:
        table_definition        // CREATE TABLE
    |
        dr_table_statement      // DR TABLE
    |
        role_definition         // CREATE ROLE
    |
        view_definition         // CREATE VIEW
    |
        index_definition        // CREATE INDEX
    |
        procedure_definition    // CREATE PROCEDURE ( AS ... | FROM CLASS ...)
    |
        export_table_definition
    |
        import_class_statement
    |
        partition_table_definition
    |
        partition_procedure_definition
    ;
    
sql_schema_manipulation_statement:
        alter_table_statement
    |   
        drop_index_statement
    |
        drop_procedure_statement
    |
        drop_role_statement
    |
        drop_table_statement
    |
        drop_view_statement
    |

        
table_definition:
        CREATE TABLE table_name '(' column_definitions ')'
    ;

column_definitions:
        column_definition (',' column_definition )*
    ; 

column_definition:
        data_definition
    |
        constraint_definition
    ;

data_definition:
        column_name type_expression  ( column_attribute )*
    ;

constraint_definition:
        CONSTRAINT constraint_name ( index_definition | limit_definition )
    ;

index_definition:
        index_type '(' column_name ( ',' column_name )* ')'
    ;

limit_definition:
        LIMIT PARTITION ROWS row_count EXECUTE delete_statement
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
        insert_or_upsert INTO table_name ( column_name_list )?
        VALUES values
    ;
        
insert_or_upsert:
        INSERT
    |
        UPSERT
    ;

delete_statement:
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

NUMBER: ( NZDIGIT ( DIGIT )* ) | ZERODIGIT;

/*
 * Note: I think this is wrong.  I think single quotes are doubled in the input.
 */
STRING_LITERAL: '\'' (~('\'' | '\\' | '\r' | '\n') | '\\' ('\'' | '\\'))* '\'';
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
TABLESAMPLE: T A B L E S A M P L E;
BERNOULLI:   B E R N O U L L I;
SYSTEM:      S Y S T E M;
BARBARA:      '||';

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
