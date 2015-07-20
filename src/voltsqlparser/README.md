# An SQL Parser

This project is an attempt to build a fully abstract front end
for the SQL database language.

# Building Requirements.
There are not really any external build requirements.  The project
uses Antlr4 to build a SQL parser and associated tree walkers, so
we need Antlr4.  However, we include the required .jar files, so there
is no searching to be done.  Maven would probably do this as well.

It's really not easy to know how to build this without Eclipse.  There
is currently no ant build file.

# Source Structure
The source for the project has several source folders in an Eclipse
Java project.

1. syntax<br/>
   This is for parser and tree walker related code.  The syntax code
   Doesn't really know any types or values, though it knows the gross
   structure of both.  It knows about interfaces.  The semantics code
   supplies the semantics of entities.
1. semantics<br/>
   This is for tree understanding code.  Here we implement types, values,
   tables, columns and symbol tables for tying them all together.
1. resources<br/>
   Resource files, such as properties files, go here.  This is to
   provide a standard location for such things, though we don't actually
   have anything here now.
1. target/generated-sources/antlr4<br/>
   This is Antlr4 generated sources.  Nothing should go here.
1. tests<br/>
   All tests go here.  We keep these in a java package hierarchy with
   the same names as syntax and semantics hierarchies so that it's easier
   to access tested code from test code.
    
   