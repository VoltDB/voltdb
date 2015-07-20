# 1. Overview
We are about implementing a front end to SQL.  SQL is a very loosely defined
language.  Much of the semantics in the language specification can be
overridden by vender implementation.  Consequently, it's difficult to
be very general.  This makes it difficult to understand exactly what
the semantics of a generic front end would be.  

We try to solve this by severely separate the front and back ends of
the language.  By *parser* here we mean the program bit which takes
a string of characters and tries to impose some sort of understanding
on it.  This is a somewhat more expansive definition of parser than
is usual, as it includes some static semantics definitions.  But it's
useful to have a term for it.  By *application* we
mean a program bit which collaborates with the parser to define the
meaning of the term *understanding*.  This is a somewhat less expansive
version of the ther application, but presumably the application
is part of a larger program which is a complete application.

For example, the entities the parser manipulates in this project are
mostly very abstract.  They are represented as Java interface pointers,
and the parser knows about their structure only through their interface
operations.  So, for example, to us a scope is an object in which
we can look up values of and define values for names.  The actual
implementation of scopes depends on the application.  In particular,
the parser has no concrete notion of a builtin set of types.  It
knows certain identifiers should denote types, and that the application
has the responsibility to provide meanings for those identifiers.

A particular and peculiar example of this abstraction is the way we
do type inference.  Typically an abstract syntax tree will have many
instances of operators applied to a set of operands, which are also
abstract syntax trees.  The operations are identifiers, or some
bit of syntax, in the SQL string.  But the identifiers or syntax
have an interpretation.  For example, the string 'SQRT' when
applied to an expression in round brackets is interpreted as the
square root function.  The argument expression must have a
numeric type as a result.  result of the call is a single or
double precision floating point number, depending on the type
of the input value.  The details of the restriction to numeric
values and the rules for the calculation of the width of the result
are both provided through the application provided interfaces. 
The type of the operation's result is inferred
from the types of the parameters, and the types of the parameters are
constrained by the operation.  The application supplies rules for
this inferring and constraining for each operation as part of the
interpretation of the operation.  

The trick to making this work is through dependency injection.
The application injects a factory into the parser when the parser
is created.  This factory
knows how to create new global scopes, how to create nested scopes, how to
create tables, indices and rows and how to create everything the parser may
need.  The objects which the factory creates have operations suitable
for the parser's use, but not much more.  The parser knows from nothing
about the real data structures.

We make use of standard Java collection classes.  We also use the estimable
Antlr4 Domain Specific Language tools, for which we thank Terrance
Parr.
## 1.1 Questions.
1. What do we do with non-standard syntax, such as Oracle and MySql's `dual`
   table, or VoltDB's `PARTITION` statements?
1. What do we do with VoltDB's restrictions, such as the restrictions on
   materialized views, or subqueries.

# 2. Syntactic Entities

## 2.1 Static Semantics Entities
### 2.1.1 Types
The parser knows nothing about types.  It gets all its types from the
application.  Mostly it sees an identifier in a syntactic context which
ought to be a type, along with ancillary supporting parameters, and it
hands the pieces, the name and parameters, to the application to translate
into a type or an error.  With that said, there are certain rules which
apply to character types or numeric types, and certain subtype relations
which must be maintained.  For the most part the application must
supply enough information to the parser to make understanding the
relations between types possible.

### 2.1.2 Scopes, or Symbol Tables
Scopes allow us to define names to be values, and to fetch the defined
value of a name.  Scopes can be nested, to implement name hiding.  For
example, a column or table alias defines the alias name to be the
column or table respectively.  But the alias can also hide the original
name of the table or column.

The parser only knows the operations on scopes.  The main operations are

1. Create a default global scope.
1. Push a new, empty scope onto the current scope.  We say the old
   scope is *nested* in the new scope, and that the new scope is
   *more inner* than the old scope.
1. Pop off the top scope. This removes, and potentially destroys, the
   innermost scope.
1. Lookup a name in the current scope and scopes in which the current scope
   is nested.
1. Define a name in the current scope to denote a given value.

The first two operations are factory operations, since they create new
scopes from old.  The latter three are operations on scopes.

### 2.1.3 Tables
1. There has to be a way for the parser to define new tables.  These
   tables need to be both named and anonymous.  For example, clearly a CREATE TABLE
   statement creates a named table.  But the statement 

    'SELECT (id AS II, x*x + y AS QUAD) from T1 as TBL;'
    
   clearly creates a table whose definition is something like 

    'CREATE TABLE <ANON> ( II integer, QUAD float ) ;'

   This means that some tables are anonymous.  But it also means that the parser
   will need a mechanism to create tables.
1. In the statement
    `select T1.id from T1, T2 where T1.V = T2.ID;`
   uses the expression `T1.id` and `T1.V`.  The value of `T1.id` is clearly
   a column, the `ID` column of `T1`.  But the table `T2` also has an `ID`
   column.  So, we need a way of ensuring that the `ID` in `T1.ID1` is not the
   `ID` in `T2.ID`.  This means tables must be some kind of scope.  But
   it's a scope which is not nested.  For example, `T1.DECIMAL` is not
   the `DECIMAL` one would get by looking in `T1`, and then in outer scopes.
   It's just a syntax error.  But, perhaps because `DECIMAL` denotes something
   in the outer scopes, the error message should be different from `T1.NODEFN`,
   where `NODFN` is not defined anywhere.

### 2.1.4 Columns and Rows
A column has a type and perhaps a name.  The name may be automatically generated.

## 2.2 Syntactic Elements
$$$ 2.2.1 Tokens as a base class.
Several syntactic elements have common structure.  Each identifier,
key word and punctuation character has a line and column number which
must be maintained for good error messages.
### 2.2.1 Identifiers and Key Words
Identifiers and Key Words have something in common.  Unlike punctuation
tokens they convey some meaning.  Identifiers may be used as lookup keys
in scopes, but Key Words probably never so used.

Note that some identifiers denote rows, some denote columns and some denote
tables.  We call this the *shape* of the variable.  Variables are often given
shapes by the grammar, so we should scrupulously maintain these shapes when
we have them.  

### 2.2.2 Literals
Literals are string representations of values.  The parser needs to hand
these strings back to the application to find out what types they are.
But it has a good idea whether they are numeric, string or enumeral
types.  So, this can can be passed back and forth, and used to validate
sensibility.
#### 2.2.2.1 Numeric Literals
These are the usual numbers.
#### 2.2.2.2 String Literals
These are the usual strings.  Presumably they are all just unicode, at least
by default.  We should have some non-ascii unicode tests.
#### 2.2.2.3 Other Literals
There may be other literals not covered here.  Time and date literals, or Boolean
literals are possible examples.  Enumerals are also examples.  These are out of
scope for the current effort.

# 3. Grammars
We make a somewhat stylized distinction between the Data Definition Language, or DDL,
the Data Manipulation Language, or DML and the Data Query Language, or DQL.  The
first consists of `CREATE TABLE` and `CREATE INDEX` commands.  The second consists
of `DROP TABLE`, `ALTER TABLE`, `UPDATE`, `INSERT` and `UPSERT` commands. 
The third consists of `SELECT` statements and of combinations of `SELECT`
statements with the set theoretic operations `UNION`, `INTERSECTION` and `NOT`.

## 3.1 Supported SQl Statements for This Iteration
1. Types
  `INTEGER`, `TINYINT`, `BIGINT`, `DECIMAL(n, p)`
  `CHAR(n)`, `VARCHAR(n)` but no collations, or maybe just case insensitivity.</br>
  Value preserving conversions between these types.
1. DDL
   `CREATE TABLE` with constraints.
   `CREATE INDEX` 
1. DML
   `DROP TABLE`
1. DQL
   `SELECT STATEMENTS` without subqueries, but with aliases, group by, where clauses
   and all kinds of joins.
