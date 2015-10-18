/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

lexer grammar SQLLexer;

@header {
}

@members {
}


/*
===============================================================================
  Tokens for Case Insensitive Keywords
===============================================================================
*/
fragment A
	:	'A' | 'a';

fragment B
	:	'B' | 'b';

fragment C
	:	'C' | 'c';

fragment D
	:	'D' | 'd';

fragment E
	:	'E' | 'e';

fragment F
	:	'F' | 'f';

fragment G
	:	'G' | 'g';

fragment H
	:	'H' | 'h';

fragment I
	:	'I' | 'i';

fragment J
	:	'J' | 'j';

fragment K
	:	'K' | 'k';

fragment L
	:	'L' | 'l';

fragment M
	:	'M' | 'm';

fragment N
	:	'N' | 'n';

fragment O
	:	'O' | 'o';

fragment P
	:	'P' | 'p';

fragment Q
	:	'Q' | 'q';

fragment R
	:	'R' | 'r';

fragment S
	:	'S' | 's';

fragment T
	:	'T' | 't';

fragment U
	:	'U' | 'u';

fragment V
	:	'V' | 'v';

fragment W
	:	'W' | 'w';

fragment X
	:	'X' | 'x';

fragment Y
	:	'Y' | 'y';

fragment Z
	:	'Z' | 'z';

/*
===============================================================================
  Reserved Keywords
===============================================================================
*/

AS : A S;
ALL : A L L;
AND : A N D;
ANY : A N Y;
ASYMMETRIC : A S Y M M E T R I C;
ASC : A S C;

BOTH : B O T H;

CASE : C A S E;
CAST : C A S T;
CREATE : C R E A T E;
CROSS : C R O S S;

DESC : D E S C;
DISTINCT : D I S T I N C T;

END : E N D;
ELSE : E L S E;
EXCEPT : E X C E P T;

FALSE : F A L S E;
FULL : F U L L;
FROM : F R O M;

GROUP : G R O U P;

HAVING : H A V I N G;

ILIKE : I L I K E;
IN : I N;
INNER : I N N E R;
INTERSECT : I N T E R S E C T;
INTO : I N T O;
IS : I S;

JOIN : J O I N;

LEADING : L E A D I N G;
LEFT : L E F T;
LIKE : L I K E;
LIMIT : L I M I T;

NATURAL : N A T U R A L;
NOT : N O T;
NULL : N U L L;

ON : O N;
OUTER : O U T E R;
OR : O R;
ORDER : O R D E R;
RIGHT : R I G H T;
SELECT : S E L E C T;
SOME : S O M E;
SYMMETRIC : S Y M M E T R I C;

TABLE : T A B L E;
THEN : T H E N;
TRAILING : T R A I L I N G;
TRUE : T R U E;

UNION : U N I O N;
UNIQUE : U N I Q U E;
USING : U S I N G;

WHEN : W H E N;
WHERE : W H E R E;
WITH : W I T H;

/*
===============================================================================
  Non Reserved Keywords
===============================================================================
*/
AVG : A V G;

BETWEEN : B E T W E E N;
BY : B Y;

CENTURY : C E N T U R Y;
CHARACTER : C H A R A C T E R;
COLLECT : C O L L E C T;
COALESCE : C O A L E S C E;
COLUMN : C O L U M N;
COUNT : C O U N T;
CUBE : C U B E;

DAY : D A Y;
DEC : D E C;
DECADE : D E C A D E;
DOW : D O W;
DOY : D O Y;
DROP : D R O P;

EPOCH : E P O C H;
EVERY : E V E R Y;
EXISTS : E X I S T S;
EXTERNAL : E X T E R N A L;
EXTRACT : E X T R A C T;


FILTER : F I L T E R;
FIRST : F I R S T;
FORMAT : F O R M A T;
FUSION : F U S I O N;

GROUPING : G R O U P I N G;

HASH : H A S H;
HOUR : H O U R;

INDEX : I N D E X;
INSERT : I N S E R T;
INTERSECTION : I N T E R S E C T I O N;
ISODOW : I S O D O W;
ISOYEAR : I S O Y E A R;

LAST : L A S T;
LESS : L E S S;
LIST : L I S T;
LOCATION : L O C A T I O N;

MAX : M A X;
MAXVALUE : M A X V A L U E;
MICROSECONDS : M I C R O S E C O N D S;
MILLENNIUM : M I L L E N N I U M;
MILLISECONDS : M I L L I S E C O N D S;
MIN : M I N;
MINUTE : M I N U T E;
MONTH : M O N T H;

NATIONAL : N A T I O N A L;
NULLIF : N U L L I F;

OVERWRITE : O V E R W R I T E;

PARTITION : P A R T I T I O N;
PARTITIONS : P A R T I T I O N S;
PRECISION : P R E C I S I O N;
PURGE : P U R G E;

QUARTER : Q U A R T E R;

RANGE : R A N G E;
REGEXP : R E G E X P;
RLIKE : R L I K E;
ROLLUP : R O L L U P;

SECOND : S E C O N D;
SET : S E T;
SIMILAR : S I M I L A R;
STDDEV_POP : S T D D E V UNDERLINE P O P;
STDDEV_SAMP : S T D D E V UNDERLINE S A M P;
SUBPARTITION : S U B P A R T I T I O N;
SUM : S U M;

TABLESPACE : T A B L E S P A C E;
THAN : T H A N;
TIMEZONE: T I M E Z O N E;
TIMEZONE_HOUR: T I M E Z O N E UNDERLINE H O U R;
TIMEZONE_MINUTE: T I M E Z O N E UNDERLINE M I N U T E;
TRIM : T R I M;
TO : T O;

UNKNOWN : U N K N O W N;
UPSERT : U P S E R T;

VALUES : V A L U E S;
VAR_SAMP : V A R UNDERLINE S A M P;
VAR_POP : V A R UNDERLINE P O P;
VARYING : V A R Y I N G;

WEEK : W E E K;

YEAR : Y E A R;

ZONE : Z O N E;


/*
===============================================================================
  Data Type Tokens
===============================================================================
*/
BOOLEAN : B O O L E A N;
BOOL : B O O L;
BIT : B I T;
VARBIT : V A R B I T;

INT1 : I N T '1';
INT2 : I N T '2';
INT4 : I N T '4';
INT8 : I N T '8';

TINYINT : T I N Y I N T; // alias for INT1
SMALLINT : S M A L L I N T; // alias for INT2
INT : I N T; // alias for INT4
INTEGER : I N T E G E R; // alias - INT4
BIGINT : B I G I N T; // alias for INT8

FLOAT4 : F L O A T '4';
FLOAT8 : F L O A T '8';

REAL : R E A L; // alias for FLOAT4
FLOAT : F L O A T; // alias for FLOAT8
DOUBLE : D O U B L E; // alias for FLOAT8

NUMERIC : N U M E R I C;
DECIMAL : D E C I M A L; // alias for number

CHAR : C H A R;
VARCHAR : V A R C H A R;
NCHAR : N C H A R;
NVARCHAR : N V A R C H A R;

DATE : D A T E;
TIME : T I M E;
TIMETZ : T I M E T Z;
TIMESTAMP : T I M E S T A M P;
TIMESTAMPTZ : T I M E S T A M P T Z;

TEXT : T E X T;

BINARY : B I N A R Y;
VARBINARY : V A R B I N A R Y;
BLOB : B L O B;
BYTEA : B Y T E A; // alias for BLOB

INET4 : I N E T '4';

// Operators
Similar_To : '~';
Not_Similar_To : '!~';
Similar_To_Case_Insensitive : '~*';
Not_Similar_To_Case_Insensitive : '!~*';

// Cast Operator
CAST_EXPRESSION
  : COLON COLON
  ;

ASSIGN  : ':=';
EQUAL  : '=';
COLON :  ':';
SEMI_COLON :  ';';
COMMA : ',';
CONCATENATION_OPERATOR : VERTICAL_BAR VERTICAL_BAR;
NOT_EQUAL  : '<>' | '!=' | '~='| '^=' ;
LTH : '<' ;
LEQ : '<=';
GTH   : '>';
GEQ   : '>=';
LEFT_PAREN :  '(';
RIGHT_PAREN : ')';
PLUS  : '+';
MINUS : '-';
MULTIPLY: '*';
DIVIDE  : '/';
MODULAR : '%';
DOT : '.';
UNDERLINE : '_';
VERTICAL_BAR : '|';
QUOTE : '\'';
DOUBLE_QUOTE : '"';

NUMBER : Digit+;

fragment
Digit : '0'..'9';

REAL_NUMBER
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
    |   '.' ('0'..'9')+ EXPONENT?
    |   ('0'..'9')+ EXPONENT
    ;

BlockComment
    :   '/*' .*? '*/' -> skip
    ;

LineComment
    :   '--' ~[\r\n]* -> skip
    ;

/*
===============================================================================
 Identifiers
===============================================================================
*/

Identifier
  : Regular_Identifier
  ;

fragment
Regular_Identifier
  : ('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|Digit|'_')*
  ;

/*
===============================================================================
 Literal
===============================================================================
*/

// Some Unicode Character Ranges
fragment
Control_Characters                  :   '\u0001' .. '\u001F';
fragment
Extended_Control_Characters         :   '\u0080' .. '\u009F';

Character_String_Literal
  : QUOTE ( ESC_SEQ | ~('\\'|'\'') )* QUOTE
  ;

fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;


/*
===============================================================================
 Whitespace Tokens
===============================================================================
*/

Space
  : ' ' -> skip
  ;

White_Space
  :	( Control_Characters  | Extended_Control_Characters )+ -> skip
  ;


BAD
  : . -> skip
  ;
