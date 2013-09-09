-- Run the basic-template against the default table in schema.py
<basic.sql>
-- then try specific cases that exercise compound indexes by filtering prefix columns.

SELECT * FROM @fromtables WHERE _variable[#leads @comparabletype] = @comparableconstant AND _variable[#follows @comparabletype] _cmp @comparableconstant

SELECT * FROM @fromtables WHERE _variable[#leads @comparabletype] = @comparableconstant AND _variable[#follows @comparabletype]  =   @comparableconstant  AND _variable[#trails @comparabletype] _cmp @comparableconstant

SELECT _variable[#order], _variable FROM @fromtables WHERE _variable[#leads @comparabletype] = @comparableconstant ORDER BY __[#order] _sortorder _optionallimitoffset

SELECT * FROM _table LHS INNER JOIN _table RHS ON LHS.@idcol = RHS.@idcol ORDER BY RHS._variable _sortorder
-- TODO: If the intent is to support a schema with multiple id-partitioned tables,
-- this statement is looking for trouble -- might want to migrate it to its own template/suite.
SELECT * FROM _table LHS INNER JOIN _table RHS ON LHS._variable[@columntype] = RHS._variable[@comparabletype]  ORDER BY RHS._variable _sortorder
