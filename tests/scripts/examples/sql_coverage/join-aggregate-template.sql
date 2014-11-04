SELECT @agg(LHS45._variable[@columntype])                                                  FROM @fromtables LHS45 ,   @fromtables RHS WHERE LHS45._variable[@columntype] = RHS._variable[@comparabletype]

SELECT LHS50._variable[#GB1 @columntype],    @agg(LHS50._variable[@columntype])            FROM @fromtables LHS50 ,   @fromtables RHS WHERE LHS50._variable[@columntype] = RHS._variable[@comparabletype] GROUP BY LHS50.__[#GB1]
SELECT LHS51._variable[#GB1 @columntype],    @agg(LHS51._variable[@columntype])            FROM @fromtables LHS51 ,   @fromtables RHS WHERE LHS51._variable[@columntype] = RHS._variable[@comparabletype] GROUP BY LHS51.__[#GB1]   ORDER BY 1   LIMIT 3

--- multiple group by columns from same tables
SELECT LHS55._variable[#GB1 @columntype],  LHS55._variable[#GB2 @columntype],     @agg(LHS55._variable[@columntype])            FROM @fromtables LHS55  @jointype   @fromtables RHS ON LHS55._variable[@columntype] = RHS._variable[@comparabletype] GROUP BY LHS55.__[#GB1], LHS55.__[#GB2]
SELECT LHS56._variable[#GB1 @columntype],  LHS56._variable[#GB2 @columntype],     @agg(LHS56._variable[@columntype])            FROM @fromtables LHS56  @jointype   @fromtables RHS ON LHS56._variable[@columntype] = RHS._variable[@comparabletype] GROUP BY LHS56.__[#GB1], LHS56.__[#GB2]   ORDER BY 1, 2   LIMIT 3

--- multiple group by columns from different tables
SELECT LHS60._variable[#GB1 @columntype],  RHS._variable[#GB2 @columntype],     @agg(LHS60._variable[@columntype]),  COUNT(*)   FROM @fromtables LHS60 @jointype   @fromtables RHS ON LHS60._variable[@columntype] = RHS._variable[@comparabletype] GROUP BY LHS60.__[#GB1], RHS.__[#GB2]
SELECT LHS61._variable[#GB1 @columntype],  RHS._variable[#GB2 @columntype],     @agg(LHS61._variable[@columntype]),  COUNT(*)   FROM @fromtables LHS61 @jointype   @fromtables RHS ON LHS61._variable[@columntype] = RHS._variable[@comparabletype] GROUP BY LHS61.__[#GB1], RHS.__[#GB2]     ORDER BY 1, 2   LIMIT 3

--- group by alias (70-75)
SELECT @onefun(LHS70._variable[@columntype]) as tag1,    @agg(LHS70._variable[@columntype])            FROM @fromtables LHS70 ,   @fromtables RHS WHERE LHS70._variable[@columntype] = RHS._variable[@comparabletype] GROUP BY tag1
SELECT LHS71._variable[#GB1 @columntype]     as tag1,    @agg(LHS71._variable[@columntype])            FROM @fromtables LHS71 ,   @fromtables RHS WHERE LHS71._variable[@columntype] = RHS._variable[@comparabletype] GROUP BY tag1

SELECT @onefun(LHS72._variable[@columntype]) as tag1,    LHS72._variable[#GB2 @columntype] as tag2,     @agg(LHS72._variable[@columntype])            FROM @fromtables LHS72  @jointype   @fromtables RHS ON LHS72._variable[@columntype] = RHS._variable[@comparabletype] GROUP BY tag1, tag2   
SELECT @onefun(LHS73._variable[@columntype]) as tag1,    LHS73._variable[#GB2 @columntype] as tag2,     @agg(LHS73._variable[@columntype])            FROM @fromtables LHS73  @jointype   @fromtables RHS ON LHS73._variable[@columntype] = RHS._variable[@comparabletype] GROUP BY tag1, tag2   ORDER BY 1, 2   LIMIT 3
