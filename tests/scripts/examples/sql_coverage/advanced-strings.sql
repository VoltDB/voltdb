{@insert_vals = "_value[id], _value[string], _value[string], _value[float]"}
{@from_tables = "_table"}
{@cmp_type = "_value[string]"}
{@assign_col = "DESC"}
{@assign_type = "_value[string]"}
-- Some day, {@optional_fn = "_pick[<options=,TRIM>]"}
-- and possibly other string-to-string functions.
-- HACK. The parser in SQLGenerator.py does not allow "<options=>" with a single empty value,
-- so, for now, the valid options are expressed as "nothing or nothing", which may
-- cause a stutter in the advanced-strings test as each "nothing" is tried in turn.
{@optional_fn = "_pick[@FN1 <options=_>]"}
{@optional_fn2 = "_pick[@FN2 <options=_>]"}
<advanced-template.sql>

select SUBSTRING ( DESC FROM @value[int:1,10] FOR @value[int:1,10] ) FROM _table WHERE  ( DESC FROM @value[int:1,10] FOR @value[int:1,10] ) > _value[string]
select DESC FROM _table ORDER BY SUBSTRING ( DESC FROM @value[int:1,10] FOR @value[int:1,10] ), DESC
