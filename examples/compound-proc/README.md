# Volt compound procedure example

This example demonstrates the use of the VoltCompoundProcedure feature, introduced in VoltDB V11.4.  A compound procedure is a non-transactional stored procedure that:

- is coded in Java
- executes in an instance of the VoltDB server
- can call other 'regular' stored procedures
- where these sub-procedures can be independently partitioned

The example has a simple illustration of the use of a compound procedure. It looks up data in two tables (supposed customer data and parts data) and uses the result to update a third table, a 'pending orders' table. There is no pretence that this is a robust ordering system; the purpose of the example is just to show the coding.

The client application is written the same way as if it were talking to a 'regular' Volt stored procedure.

For more information, see under VoltCompoundProcedure in the VoltDB server Javadoc.
