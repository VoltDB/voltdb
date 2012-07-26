{@insert_vals = "_value[id], _value[string], _value[string], _value[float]"}
{@from_tables = "_table"}
{@col_type = "string"}
{@cmp_type = "_value[string]"}
{@assign_col = "DESC"}
{@assign_type = "_value[string]"}
-- Some day, {@optional_fn = "_pick[<options=,TRIM>]"}
-- and possibly other string-to-string functions.
-- HACK. The parser in SQLGenerator.py does not allow "<options=>" with a single empty value,
-- so, instead, logic specific to _pick treats "<options=_> as "<options=>" a single no-op nothing option.
{@optional_fn = "_pick[@FN1 <options=_>]"}
{@optional_fn2 = "_pick[@FN2 <options=_>]"}
<advanced-template.sql>

SELECT SUBSTRING ( DESC FROM @value[int:1,10] FOR @value[int:1,10] ) substrQ1 FROM _table ORDER BY DESC
SELECT DESC substrQ2 FROM _table WHERE    SUBSTRING ( DESC FROM @value[int:1,10] FOR @value[int:1,10] ) > _value[string] ORDER BY DESC
SELECT DESC substrQ3 FROM _table ORDER BY SUBSTRING ( DESC FROM @value[int:1,10] ), DESC

{@prefix_patterns = "_pick[<options='abc%','%','abc!%%'>]"}
{@patterns1 = "_pick[<options='!%','abc!%','abc!%%','abc%z','%z','!%z','abc!%z','abc!%%z','abc%!%z','abc'>]"}
{@patterns2 = "_pick[<options='!_','abc!_','abc!__','abc_z','_z','!_z','abc!_z','abc!__z','abc_!_z','abc_','_'>]"}
{@patterns3 = "_pick[<options='!_%','abc!_%','abc!_%%','abc_%z','_%z','!_%z','abc!_%z','abc!_%%z','abc_%!%z','abc_%','_%'>]"}
{@patterns4 = "_pick[<options='!%_','abc!%_','abc!%__','abc%_z','%_z','!%_z','abc!%_z','abc!%__z','abc%_!_z','abc%_','%_'>]"}
{@patterns5 = "_pick[<options='!%%','abc!%%','abc!%%%','abc%%z','%%z','!%%z','abc!%%z','abc!%%%z','abc%%!%z','abc%%','%%'>]"}
{@patterns6 = "_pick[<options='!__','abc!__','abc!___','abc__z','__z','!__z','abc!__z','abc!___z','abc__!_z','abc__','__'>]"}

SELECT @assign_col likeQ10 FROM _table WHERE @assign_col _like @prefix_patterns ESCAPE '!'
SELECT @assign_col likeQ11 FROM _table WHERE @assign_col _like @patterns1
SELECT @assign_col likeQ12 FROM _table WHERE @assign_col _like @patterns2
SELECT @assign_col likeQ13 FROM _table WHERE @assign_col _like @patterns3
SELECT @assign_col likeQ14 FROM _table WHERE @assign_col _like @patterns4
SELECT @assign_col likeQ15 FROM _table WHERE @assign_col _like @patterns5
SELECT @assign_col likeQ16 FROM _table WHERE @assign_col _like @patterns6

SELECT @assign_col likeQ20 FROM _table WHERE @assign_col _like @prefix_patterns ESCAPE '!'
-- SELECT @assign_col likeQ21 FROM _table WHERE @assign_col _like @patterns1 ESCAPE '!'
-- SELECT @assign_col likeQ22 FROM _table WHERE @assign_col _like @patterns2 ESCAPE '!'
-- SELECT @assign_col likeQ23 FROM _table WHERE @assign_col _like @patterns3 ESCAPE '!'
-- SELECT @assign_col likeQ24 FROM _table WHERE @assign_col _like @patterns4 ESCAPE '!'
-- SELECT @assign_col likeQ25 FROM _table WHERE @assign_col _like @patterns5 ESCAPE '!'
-- SELECT @assign_col likeQ26 FROM _table WHERE @assign_col _like @patterns6 ESCAPE '!'
