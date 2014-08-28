<grammar.sql>

{_partitionedsourcetable |= "SOURCE_P1"}
{_partitionedsourcetable |= "SOURCE_P2"}

{_replicatedsourcetable |= "SOURCE_R1"}

{_sourcetable |= "_partitionedsourcetable"}
{_sourcetable |= "_replicatedsourcetable"}

{_targettable |= "TARGET_P1"}

{_filter_subq |= "SELECT * FROM _sourcetable WHERE _integercolumnpredicate"}
{_agg_subq |= "SELECT ID, _genericagg(_variable[string]), _numagg(_variable[int]), _numagg(_variable[float]) FROM _sourcetable GROUP BY ID"}

{_distinct_agg_subq |= "SELECT ID, _genericagg(_variable[string]), _distinctableagg(_distinct _variable[int]), _distinctableagg(_distinct _variable[float]) FROM _sourcetable WHERE ID = _value[int:1,5] GROUP BY ID"}
{_distinct_agg_subq |= "SELECT ID, _genericagg(_variable[string]), _distinctableagg(_distinct _variable[int]), _distinctableagg(_distinct _variable[float]) FROM _replicatedsourcetable GROUP BY ID"}
-- queries containing distinct aggregates (e.g., count(distinct c1)) are executed as multi-fragment
-- Even when grouping by partition key (ENG-6826)
-- {_distinct_agg_subq |= "SELECT ID, _genericagg(_variable[string]), _distinctableagg(_distinct _variable[int]), _distinctableagg(_distinct _variable[float]) FROM _sourcetable GROUP BY ID"}

{_having_subq |= "SELECT ID, _genericagg(_variable[string]), _numagg(_variable[int]), COUNT(_variable[#have]) FROM _sourcetable GROUP BY ID HAVING COUNT(__[#have]) > _value[int:0,10]"}

{_limit_subq |= "SELECT SRC1.ID, _variable[int], _variable[numeric], _variable[numeric] FROM _replicatedsourcetable AS SRC1 ORDER BY 1, 2, 3, 4 LIMIT 8"}
{_limit_subq |= "SELECT SRC1.ID, _variable[int], _variable[numeric], _variable[numeric] FROM _replicatedsourcetable AS SRC1 ORDER BY 1, 2, 3, 4 LIMIT 4 OFFSET _value[int:0,30]"}
{_limit_subq |= "SELECT SRC1.ID, _variable[int], _variable[numeric], _variable[numeric] FROM _sourcetable AS SRC1 WHERE ID = _value[int:100,102] ORDER BY 1, 2, 3, 4 LIMIT 4 OFFSET _value[int:0,30]"}

{_distinct_subq |= "SELECT DISTINCT SRC1.ID FROM _sourcetable AS SRC1 WHERE ID = _value[int:1,10]"}
{_distinct_subq |= "SELECT DISTINCT SRC1.NUM FROM _replicatedsourcetable AS SRC1 WHERE SRC1.NUM IS NOT NULL"}

-- Avoid combinatorial explosion
{_subq |= "_filter_subq"}
{_subq |= "_agg_subq"}
-- {_subq |= "_distinct_agg_subq"}
-- {_subq |= "_having_subq"}
{_subq |= "_limit_subq"}

{@insertvals = "_id, _value[string], _value[int32 null30], _value[float]"}

-- clear everything out first
TRUNCATE TABLE _sourcetable
TRUNCATE TABLE _targettable

-- insert into source tables
-- Choose some constant values so joins will produce rows
INSERT INTO _sourcetable VALUES (100, _value[string], _value[int32 null30], _value[float])
INSERT INTO _sourcetable VALUES (101, _value[string], _value[int32 null30], _value[float])
INSERT INTO _sourcetable VALUES (102, _value[string], _value[int32 null30], _value[float])
INSERT INTO _sourcetable VALUES (103, _value[string], _value[int32 null30], _value[float])
INSERT INTO _sourcetable VALUES (104, _value[string], _value[int32 null30], _value[float])
INSERT INTO _sourcetable VALUES (@insertvals)

-- filter
INSERT INTO _targettable _filter_subq

-- Agg
INSERT INTO _targettable _agg_subq

-- Agg with distinct
INSERT INTO _targettable _distinct_agg_subq

-- Having
INSERT INTO _targettable _having_subq

-- Join
INSERT INTO _targettable SELECT SRC1.ID, SRC2.DESC, SRC1.NUM, SRC2.RATIO FROM _sourcetable AS SRC1, _sourcetable AS SRC2 WHERE SRC1.ID = SRC2.ID

-- Limit/offset
-- We get rounding differences here if we cast float to string
-- order by/limit can introduce 2-fragment plans for subquery
INSERT INTO _targettable _limit_subq

-- Select distinct
INSERT INTO _targettable (ID) SELECT DISTINCT SRC1.ID FROM _sourcetable AS SRC1 WHERE ID = _value[int:1,10]
INSERT INTO _targettable (ID) SELECT DISTINCT SRC1.NUM FROM _replicatedsourcetable AS SRC1 WHERE SRC1.NUM IS NOT NULL

-- dump target, and empty it
SELECT * FROM _targettable ORDER BY 1, 2, 3, 4
TRUNCATE TABLE _targettable

-- Subqueries
INSERT INTO _targettable SELECT SQ1.ID, SRC2.DESC, SQ1.NUM, SRC2.RATIO FROM (_subq) AS SQ1(ID, DESC, NUM, RATIO), _sourcetable AS SRC2(ID, DESC, NUM, RATIO) WHERE SQ1.ID = SRC2.ID

-- dump target, and empty it
SELECT * FROM _targettable ORDER BY 1, 2, 3, 4
TRUNCATE TABLE _targettable

INSERT INTO _targettable SELECT SQ1.ID, SQ2.DESC, SQ1.NUM, SQ2.RATIO FROM (_subq) AS SQ1(ID, DESC, NUM, RATIO), (_agg_subq) AS SQ2(ID, DESC, NUM, RATIO) WHERE SQ1.ID = SQ2.ID

-- dump target, and empty it
SELECT * FROM _targettable ORDER BY 1, 2, 3, 4
TRUNCATE TABLE _targettable
