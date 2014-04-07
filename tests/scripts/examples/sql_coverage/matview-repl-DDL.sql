CREATE TABLE R1 (
  ID INTEGER NOT NULL,
  WAGE SMALLINT,
  DEPT SMALLINT,
  AGE SMALLINT,
  RENT SMALLINT,
  PRIMARY KEY (ID)
);

CREATE VIEW V_R1 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS 
	SELECT wage, dept, count(*), sum(age), sum(rent)
    FROM R1
	GROUP BY wage, dept;


CREATE TABLE R2 (
  ID INTEGER NOT NULL,
  WAGE SMALLINT,
  DEPT SMALLINT,
  AGE SMALLINT,
  RENT SMALLINT,
  PRIMARY KEY (ID)
);

CREATE VIEW V_R2 (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS 
	SELECT wage, dept, count(*), sum(age), sum(rent)
    FROM R2 
	GROUP BY wage, dept;

CREATE VIEW V_R2_ABS (V_G1, V_G2, V_CNT, V_sum_age, V_sum_rent) AS 
	SELECT ABS(wage), dept, count(*), sum(age), sum(rent)
    FROM R2 
	GROUP BY ABS(wage), dept;

--- This table is for testing three table joins, as mv partition table can only join with two more replicated tables.	
CREATE TABLE R2V (
  V_G1 INTEGER NOT NULL,
  V_G2 SMALLINT,
  V_CNT SMALLINT,
  V_sum_age SMALLINT,
  V_sum_rent SMALLINT,
  PRIMARY KEY (V_G1)
);

