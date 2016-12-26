<grammar.sql>
-- Purge and generate data first.
DELETE FROM _table
--INSERT
-- test basic INSERT
INSERT INTO _table VALUES (_id, 'desc_random', _value[int16], _value[float])
INSERT INTO _table VALUES (1000, 'desc_1000', 1000, 1000.5)
INSERT INTO _table VALUES (1001, 'desc_1000', 1000, 1000.5)
INSERT INTO _table VALUES (1010, 'desc_1010', 1010, 1010.5)
INSERT INTO _table VALUES (1011, 'desc_1010', 1010, 1010.5)
-- Purposely excluding rows from some _tables to tease out different cases.
INSERT INTO P1 VALUES (1020, 'desc_1020', 1020, 1020.5)
INSERT INTO R1 VALUES (1020, 'desc_1020', 1020, 1020.5)

-- test SET Operations
  SELECT _variable[#same] FROM _table _setop  SELECT __[#same] FROM _table
  SELECT _variable[#same] FROM _table _setop  SELECT __[#same] FROM _table   _setop SELECT __[#same] FROM _table
( SELECT _variable[#same] FROM _table _setop  SELECT __[#same] FROM _table ) _setop SELECT __[#same] FROM _table
  SELECT _variable[#same] FROM _table _setop (SELECT __[#same] FROM _table   _setop SELECT __[#same] FROM _table )

-- Order by
  SELECT _variable[#same] as tag FROM _table _setop  SELECT __[#same] FROM _table ORDER BY tag
  SELECT _variable[#same] as tag FROM _table _setop  SELECT __[#same] FROM _table ORDER BY tag LIMIT 3 OFFSET 1
  SELECT ABS(_variable[#same]) as tag FROM _table _setop  SELECT __[#same] FROM _table ORDER BY tag
