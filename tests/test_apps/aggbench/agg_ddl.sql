-- User FACT Table
CREATE TABLE TB1 ( 
  timestamp_id BIGINT NOT NULL,               
  C2 INT NOT NULL,                     
  C3 TINYINT NOT NULL,              
  C4 BIGINT NOT NULL,                    
  C5 BIGINT NOT NULL,                 
  C6 BIGINT NOT NULL,                 
  C7 BIGINT NOT NULL,              
  C8 BIGINT NOT NULL,              
  C9 BIGINT NOT NULL,                    
  C10 TINYINT NOT NULL,                    
  C11 BIGINT NOT NULL,                     
  C12 INT NOT NULL,                   
  C13 TINYINT NOT NULL,             
  TB3_id INT NOT NULL,                
  TB2_id BIGINT NOT NULL,                    
  C16 TINYINT NOT NULL,              
  C17 INT NOT NULL,                   
  C18 BIGINT NOT NULL,                
  TB4_id BIGINT NOT NULL,                 
  C20 SMALLINT NOT NULL,               
  bytes FLOAT,                       
  C22 FLOAT,                        
  C23 FLOAT,                     
  C24 FLOAT,                  
  C25 FLOAT,                   
  C26 FLOAT,                
  C27 FLOAT,                 
  C28 FLOAT,                 
  C29 FLOAT             
);
PARTITION TABLE TB1 ON COLUMN TB2_id;

CREATE INDEX user_info_TB2_id_timestamp_id_idx ON TB1 (TB2_id, timestamp_id);
CREATE INDEX user_info_timestamp_id_idx ON TB1 (timestamp_id);
CREATE INDEX user_info_imsi_iapplication_info_id_idx ON TB1 (TB3_id, TB2_id);
CREATE INDEX user_info_TB4_id_idx ON TB1 (TB4_id, TB2_id, timestamp_id);

-- subscriber info Table
CREATE TABLE TB2 (
  TB2_id BIGINT  NOT NULL,                                 
  C2 BIGINT,                              
  C3 BIGINT,                                
  C4 TINYINT,                             
  userlabel VARCHAR(255),                               
  C6 VARCHAR(63),                                  
  corporate VARCHAR(255),                               
  billing_type INT,                               
  name VARCHAR(255),                                    
  C10 VARCHAR(255),                                  
  C11 VARCHAR(255),                                    
  C12 BIGINT,                        
  C13 BIGINT  
  , PRIMARY KEY (TB2_id),                            
);
PARTITION TABLE TB2 ON COLUMN TB2_id;

CREATE INDEX TB2_billing_type_idx ON TB2 (billing_type);
CREATE INDEX TB2_corporate_idx ON TB2 (corporate);


-- application_info
CREATE TABLE TB3 (
  TB3_id INT NOT NULL,
  userlabel VARCHAR(255),
  C3 VARCHAR(1023),
  C4 TINYINT,
  C5 INT,
  PRIMARY KEY ( TB3_id )
);

-- handset_info
CREATE TABLE TB4 (
  TB4_id BIGINT NOT NULL,
  C2 BIGINT,
  C3 BIGINT,
  C4 TINYINT,
  userlabel VARCHAR(255),
  model VARCHAR(255),
  manufacturer VARCHAR(255),
  os VARCHAR(255),
  C9 VARCHAR(255),
  C10 TINYINT,
  C11 BIGINT,
  PRIMARY KEY (TB4_id)
);

CREATE PROCEDURE Q1  AS select sum(bytes) from TB1;
CREATE PROCEDURE Q2  AS select TB2_id, count(TB2_id) from TB1 group by TB2_id limit 10;
CREATE PROCEDURE Q3  AS select TB1.TB2_id, count(TB1.TB2_id) imsi_count from TB1 group by TB2_id order by imsi_count desc limit 10;

CREATE PROCEDURE Q4 AS select TB2.userlabel label, t1.TB2_id, sum(imsi_count) imsi_count from (select TB2_id, count(TB2_id) imsi_count from TB1 group by TB2_id) t1, TB2 where TB2.TB2_id = t1.TB2_id group by t1.TB2_id, TB2.userlabel order by imsi_count desc limit 10;
-- CREATE PROCEDURE Q4_original AS select TB2.userlabel label, TB1.TB2_id, count(TB1.TB2_id) imsi_count from TB1, TB2 where TB2.TB2_id = TB1.TB2_id group by TB2_id, TB2.userlabel order by imsi_count desc limit 10;

CREATE PROCEDURE Q5  AS select coalesce( t2.userlabel, 'UNKNOWN' ) label, t1.TB2_id imsi, sum(t1.imsi_count) imsi_count from (select TB2_id, count(TB2_id) imsi_count from TB1 group by TB2_id) t1 LEFT OUTER JOIN TB2 t2 ON t1.TB2_id = t2.TB2_id group by t1.TB2_id, t2.userlabel order by imsi_count desc limit 10;
-- CREATE PROCEDURE Q5_original  AS select coalesce( t2.userlabel, 'UNKNOWN' ) label, t1.TB2_id imsi, count(t1.TB2_id) imsi_count from TB1 t1 LEFT OUTER JOIN TB2 t2 ON t1.TB2_id = t2.TB2_id group by t1.TB2_id, t2.userlabel order by imsi_count desc limit 10;

CREATE PROCEDURE Q6  AS select TB2_id imsi, sum( bytes ) volume from TB1 group by (TB2_id) order by volume desc limit 10;
CREATE PROCEDURE Q7  AS select TB2_id imsi, sum( bytes ) volume from TB1 group by (TB2_id) order by volume desc limit 10 offset 10;
CREATE PROCEDURE Q8  AS select TB2_id imsi, sum( bytes ) volume from TB1 group by (TB2_id) order by volume desc limit 20;

CREATE PROCEDURE Q9  AS select t1.TB2_id imsi, sum(sum_bytes) volume from (select TB2_id, sum(bytes) sum_bytes from TB1 group by TB2_id) t1 LEFT OUTER JOIN TB2 t2 ON t1.TB2_id = t2.TB2_id group by t1.TB2_id order by volume desc limit 20;
-- CREATE PROCEDURE Q9_original  AS select t1.TB2_id imsi, sum( t1.bytes ) volume from TB1 t1 LEFT OUTER JOIN TB2 t2 ON t1.TB2_id = t2.TB2_id group by t1.TB2_id order by volume desc limit 20;

CREATE PROCEDURE Q10 AS select TB2.BILLING_TYPE BILLING_TYPE, sum(BYTES) VOLUME from TB1, TB2 where TB1.TB2_id = TB2.TB2_id AND TB2.BILLING_TYPE >= 90 group by (TB2.BILLING_TYPE);
CREATE PROCEDURE Q11 AS select hour( from_unixtime( TB1.timestamp_id / 1000 ) ) time_hour,  TB2.corporate corporate, sum(TB1.bytes) volume from TB1, TB2  where TB1.TB2_id = TB2.TB2_id AND TB2.corporate = '44' group by corporate, hour( from_unixtime( TB1.timestamp_id / 1000 ) );
CREATE PROCEDURE Q12 AS select hour( from_unixtime(TB1.timestamp_id / 1000  ) ) time_hour, minute( from_unixtime( TB1.timestamp_id / 1000 ) ) time_minute, TB2.corporate corporate, sum(TB1.bytes) volume from TB1, TB2 where TB1.TB2_id = TB2.TB2_id AND TB2.corporate = '44' group by corporate, hour( from_unixtime(TB1.timestamp_id / 1000  ) ), minute( from_unixtime( TB1.timestamp_id / 1000 ) );

CREATE PROCEDURE Q13 AS select concat( TB4.manufacturer, ' ', TB4.model ) model, count( model ) model_count, sum( bytes) volume  from (select TB4_id, sum( bytes) bytes from TB1 group by TB4_id) TB1, TB4  where TB1.TB4_id = TB4.TB4_id  group by concat( TB4.manufacturer, ' ', TB4.model )  order by volume, model_count desc limit 20;

CREATE PROCEDURE Q14 AS select * from TB1 where TB2_id = 213031002743386;
CREATE PROCEDURE Q15 AS select hour( from_unixtime( TB1.timestamp_id / 1000 ) ) time_hour from TB1 GROUP BY hour( from_unixtime( TB1.timestamp_id / 1000 ));
CREATE PROCEDURE Q16 AS SELECT HOUR( from_unixtime( TB1.timestamp_id / 1000  ) ) time_hour, minute( from_unixtime( TB1.timestamp_id / 1000  ) ) time_minute, TB1.TB2_id TB2_id, TB2.userlabel name, sum( TB1.bytes ) volume from TB1, TB2 WHERE TB1.TB2_id = TB2.TB2_id AND TB1.TB2_id = 213031002743386 AND TB1.timestamp_id >= 1396332000122 AND TB1.timestamp_id <= 1396335599360 GROUP BY HOUR( from_unixtime( TB1.timestamp_id / 1000  ) ),  minute( from_unixtime( TB1.timestamp_id / 1000  )), TB1.TB2_id, TB2.userlabel ORDER BY time_hour, time_minute LIMIT 60;
CREATE PROCEDURE Q17 AS select TB3_id, TB2_id from TB1 where TB2_id = 213031002743386 group by TB2_id, TB3_id limit 10;
CREATE PROCEDURE Q18 AS select coalesce( t2.userlabel, 'UNKNOWN') user_name, t1.TB2_id user_id, coalesce( t3.userlabel, 'UNKNOWN') app_name, t1.TB3_id app_id, sum( t1.bytes ) volume  from TB1 t1 LEFT OUTER JOIN TB2 t2 ON t1.TB2_id = t2.TB2_id  LEFT OUTER JOIN TB3 t3 ON t1.TB3_id = t3.TB3_id where  t1.TB2_id = 213031002743386 group by t1.TB3_id, t1.TB2_id, coalesce( t3.userlabel, 'UNKNOWN'), coalesce( t2.userlabel, 'UNKNOWN') order by volume desc limit 10;

-- rewrite with subquery
CREATE PROCEDURE Q19 AS select coalesce( t2.userlabel, 'UNKNOWN') user_name, t1.TB2_id user_id,  coalesce( t3.userlabel, 'UNKNOWN') app_name, t1.TB3_id app_id, sum( t1.sum_bytes ) volume  from (select TB2_id, TB3_id, sum(bytes) sum_bytes from TB1 group by TB2_id, TB3_id ) t1 LEFT OUTER JOIN TB2 t2 ON t1.TB2_id = t2.TB2_id  LEFT OUTER JOIN TB3 t3 ON t1.TB3_id = t3.TB3_id group by t1.TB3_id, coalesce( t3.userlabel, 'UNKNOWN'), t1.TB2_id, coalesce( t2.userlabel,'UNKNOWN') order by volume desc limit 10;

-- better with all the optimizations (ordered subquery, limit push down)
-- CREATE PROCEDURE Q19_rewrite_2 AS select coalesce( t2.userlabel, 'UNKNOWN') user_name, t1.TB2_id user_id,  coalesce( t3.userlabel, 'UNKNOWN') app_name, t1.TB3_id app_id, sum( t1.sum_bytes ) volume  from (select TB2_id, TB3_id, sum(bytes) sum_bytes from TB1 group by TB2_id, TB3_id order by sum_bytes desc) t1 LEFT OUTER JOIN TB2 t2 ON t1.TB2_id = t2.TB2_id  LEFT OUTER JOIN TB3 t3 ON t1.TB3_id = t3.TB3_id group by t1.TB3_id, coalesce( t3.userlabel, 'UNKNOWN'), t1.TB2_id, coalesce( t2.userlabel,'UNKNOWN') order by volume desc limit 10;
-- CREATE PROCEDURE Q19_original AS select  coalesce( t2.userlabel, 'UNKNOWN' ) user_name, t1.TB2_id user_id,  coalesce( t3.userlabel, 'UNKNOWN' ) app_name, t1.TB3_id app_id,  sum( t1.bytes ) volume from      TB1 t1  LEFT OUTER JOIN TB2 t2 ON t1.TB2_id = t2.TB2_id LEFT OUTER JOIN TB3 t3 ON t1.TB3_id = t3.TB3_id group by t1.TB3_id, t1.TB2_id, t3.userlabel, t2.userlabel order by volume desc limit 10;

CREATE PROCEDURE Q20 AS SELECT hour( from_unixtime( TB1.timestamp_id / 1000 ) ) time_hour, minute( from_unixtime( TB1.timestamp_id / 1000 ) ) time_minute,  coalesce( TB4.os, 'UNKNOWN') os,  sum( TB1.bytes ) volume  from TB1  LEFT OUTER JOIN TB4 TB4 ON TB1.TB4_id = TB4.TB4_id WHERE TB1.timestamp_id >= 1396332000122 AND TB1.timestamp_id <= 1396335599360 GROUP BY hour( from_unixtime( TB1.timestamp_id / 1000 ) ), minute( from_unixtime( TB1.timestamp_id / 1000 ) ), coalesce( TB4.os, 'UNKNOWN') ORDER BY time_hour, time_minute LIMIT 60;



