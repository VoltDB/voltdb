<configure-for-ints.sql>
<numeric-template.sql>

-- These caused frequent failures in numeric-template due to floating point rounding, so keep them here
SELECT @optionalfn(_variable[numeric])     _math      1.0 + (_value[float]     )  AS numint1 FROM @fromtables
SELECT @optionalfn(_variable[numeric])     _math @optionalfn(_variable[numeric])  AS numint2 FROM @fromtables
SELECT _numagg(_numfun(_variable[numeric])), _numagg(_numfun(_variable[numeric])) AS numint3 FROM @fromtables

