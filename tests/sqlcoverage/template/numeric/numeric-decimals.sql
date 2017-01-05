<grammar.sql>
-- This file defines minimal macros to drive numeric-template.sql
{@insertvals = "_id, _value[decimal], _value[decimal null20], _value[float]"}
{@dmltable = "_table"}
{@fromtables = "_table"}
{@optionalfn = "_numfun"}

<numeric-template.sql>
