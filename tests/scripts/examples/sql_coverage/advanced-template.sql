-- Temporarily, this file holds patterns that are more complex or
-- differ from the SQL tested in the basic-template.sql file

-- DML, generate random data first.
INSERT INTO _table VALUES (@insert_vals)


SELECT * FROM @from_tables WHERE (_variable[int]) IN (_value[int:1,3], _value[int:1,5])
SELECT * FROM @from_tables WHERE (_variable[string]) IN (_value[string], _value[string])
