<configure-default.sql>
<advanced-template.sql>


-- {@columntype = "int"}
SELECT * FROM @fromtables A WHERE CASE WHEN A._variable[@columntype] _cmp @comparableconstant THEN A._variable[@columntype] ELSE A._variable[@columntype] * 10  END _cmp @comparableconstant + 10
-- SELECT * FROM @fromtables A WHERE CASE WHEN A._variable[@columntype] _cmp @comparableconstant THEN A._variable[@columntype] END _cmp @comparableconstant + 10
SELECT _variable[@comparabletype], CASE WHEN A._variable[@columntype] _cmp @comparableconstant THEN A._variable[@columntype] ELSE A._variable[@columntype] * 10 END FROM @fromtables WHERE @columnpredicate
SELECT _variable[@comparabletype], CASE WHEN A._variable[@columntype] _cmp @comparableconstant THEN A._variable[@columntype] END FROM @fromtables WHERE @columnpredicate


SELECT * FROM @fromtables A WHERE CASE A._variable[@columntype] WHEN @comparableconstant THEN A._variable[@columntype] * 2 ELSE A._variable[@columntype] * 10  END _cmp @comparableconstant + 10
-- SELECT * FROM @fromtables A WHERE CASE A._variable[@columntype] WHEN @comparableconstant THEN A._variable[@columntype] * 2 END _cmp @comparableconstant + 10
SELECT _variable[@comparabletype], CASE A._variable[@columntype] WHEN  @comparableconstant THEN A._variable[@columntype] * 2 ELSE A._variable[@columntype] * 10  END  FROM @fromtables WHERE @columnpredicate
SELECT _variable[@comparabletype], CASE A._variable[@columntype] WHEN  @comparableconstant THEN A._variable[@columntype] * 2 END  FROM @fromtables WHERE @columnpredicate