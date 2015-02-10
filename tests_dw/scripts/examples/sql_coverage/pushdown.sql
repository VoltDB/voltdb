<grammar.sql>
-- DML, generate random data first

DELETE FROM _table
-- Hard-code a couple of negative IDs into each table to force multiple matches outside the normal
-- generated id space -- to avoid primary constraint violations, don't use _value in these lines
INSERT INTO _table VALUES(-1,  'match',        88,            88.88);
INSERT INTO _table VALUES(-2,  'match',        88,            88.88);

INSERT INTO _table VALUES(_id, _value[string], _value[int16 null20], _value[float]);

{_optionallimitoffset |= "limit 2"}
{_optionallimitoffset |= "limit 1 offset 1"}
--We don't support this -- is it standard SQL?
--{_optionallimitoffset |= "        offset 1"}

-- Distribute an optional limit/offset on an optionally distinct scan of an optionally partitioned table
SELECT _distinct _variable FROM _table ORDER BY 1 _optionallimitoffset

-- Throw in a group by and an aggregate
SELECT _variable[#grouped], _numagg(_variable[numeric]) from _table group by __[#grouped] order by 1, 2 _optionallimitoffset

-- Combine a join of optionally partitioned tables with an optional limit/offset.
-- Stick to ID equality join to give two partitioned tables a fighting chance.
SELECT T1.ID, T2._variable from _table T1, _table T2 where T1.ID = T2.ID order by 1, 2 _optionallimitoffset
