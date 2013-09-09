<grammar.sql>
-- This file defines minimal macros to drive numeric-template.sql
{@insertvals = "_id, _value[byte], _value[int16], _value[int32]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@optionalfn = "_numfun"}

<numeric-template.sql>

-- These caused frequent failures in numeric-template due to floating point rounding, so keep them here
SELECT @optionalfn(_variable[numeric]) _math      1.0 + (_value[float]     ) as numint1 FROM @fromtables
SELECT @optionalfn(_variable[numeric]) _math @optionalfn(_variable[numeric]) as numint2 FROM @fromtables
