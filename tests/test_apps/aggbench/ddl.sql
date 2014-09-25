-- User-Plane FACT Table
CREATE TABLE user_info ( 
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
  application_id INT NOT NULL,                
  sub_id BIGINT NOT NULL,                    
  C16 TINYINT NOT NULL,              
  C17 INT NOT NULL,                   
  C18 BIGINT NOT NULL,                
  handset_id BIGINT NOT NULL,                 
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
PARTITION TABLE user_info ON COLUMN sub_id;

CREATE INDEX user_info_sub_id_timestamp_id_idx ON user_info (sub_id, timestamp_id);
CREATE INDEX user_info_timestamp_id_idx ON user_info (timestamp_id);
CREATE INDEX user_info_imsi_iapplication_info_id_idx ON user_info (application_id, sub_id);
CREATE INDEX user_info_handset_id_idx ON user_info (handset_id, sub_id, timestamp_id);

-- Subscriber Dimension Table
CREATE TABLE subscriber_info (
  sub_id BIGINT  NOT NULL,                                 
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
  , PRIMARY KEY (sub_id),                            
);
PARTITION TABLE subscriber_info ON COLUMN sub_id;

CREATE INDEX subscriber_info_billing_type_idx ON subscriber_info (billing_type);
CREATE INDEX subscriber_info_corporate_idx ON subscriber_info (corporate);


-- application_info
CREATE TABLE application_info (
  application_id INT NOT NULL,
  userlabel VARCHAR(255),
  C3 VARCHAR(1023),
  C4 TINYINT,
  C5 INT,
  PRIMARY KEY ( application_id )
);

-- handset_info
CREATE TABLE handset_info (
  handset_id BIGINT NOT NULL,
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
  PRIMARY KEY (handset_id)
);

CREATE PROCEDURE Q1  AS select sum(bytes) from user_info;
CREATE PROCEDURE Q2  AS select sub_id, count(sub_id) from user_info group by sub_id limit 10;
CREATE PROCEDURE Q3  AS select user_info.sub_id, count(user_info.sub_id) imsi_count from user_info group by sub_id order by imsi_count desc limit 10;

CREATE PROCEDURE Q4 AS select subscriber_info.userlabel label, t1.sub_id, sum(imsi_count) imsi_count from (select sub_id, count(sub_id) imsi_count from user_info group by sub_id) t1, subscriber_info where subscriber_info.sub_id = t1.sub_id group by t1.sub_id, subscriber_info.userlabel order by imsi_count desc limit 10;
-- CREATE PROCEDURE Q4_original AS select subscriber_info.userlabel label, user_info.sub_id, count(user_info.sub_id) imsi_count from user_info, subscriber_info where subscriber_info.sub_id = user_info.sub_id group by sub_id, subscriber_info.userlabel order by imsi_count desc limit 10;

CREATE PROCEDURE Q5  AS select coalesce( t2.userlabel, 'UNKNOWN' ) label, t1.sub_id imsi, sum(t1.imsi_count) imsi_count from (select sub_id, count(sub_id) imsi_count from user_info group by sub_id) t1 LEFT OUTER JOIN subscriber_info t2 ON t1.sub_id = t2.sub_id group by t1.sub_id, t2.userlabel order by imsi_count desc limit 10;
-- CREATE PROCEDURE Q5_original  AS select coalesce( t2.userlabel, 'UNKNOWN' ) label, t1.sub_id imsi, count(t1.sub_id) imsi_count from user_info t1 LEFT OUTER JOIN subscriber_info t2 ON t1.sub_id = t2.sub_id group by t1.sub_id, t2.userlabel order by imsi_count desc limit 10;

CREATE PROCEDURE Q6  AS select sub_id imsi, sum( bytes ) volume from user_info group by (sub_id) order by volume desc limit 10;
CREATE PROCEDURE Q7  AS select sub_id imsi, sum( bytes ) volume from user_info group by (sub_id) order by volume desc limit 10 offset 10;
CREATE PROCEDURE Q8  AS select sub_id imsi, sum( bytes ) volume from user_info group by (sub_id) order by volume desc limit 20;

CREATE PROCEDURE Q9  AS select t1.sub_id imsi, sum(sum_bytes) volume from (select sub_id, sum(bytes) sum_bytes from user_info group by sub_id) t1 LEFT OUTER JOIN subscriber_info t2 ON t1.sub_id = t2.sub_id group by t1.sub_id order by volume desc limit 20;
-- CREATE PROCEDURE Q9_original  AS select t1.sub_id imsi, sum( t1.bytes ) volume from user_info t1 LEFT OUTER JOIN subscriber_info t2 ON t1.sub_id = t2.sub_id group by t1.sub_id order by volume desc limit 20;

CREATE PROCEDURE Q10 AS select subscriber_info.BILLING_TYPE BILLING_TYPE, sum(BYTES) VOLUME from user_info, subscriber_info where user_info.sub_id = subscriber_info.sub_id AND subscriber_info.BILLING_TYPE >= 90 group by (subscriber_info.BILLING_TYPE);
CREATE PROCEDURE Q11 AS select hour( from_unixtime( user_info.timestamp_id / 1000 ) ) time_hour,  subscriber_info.corporate corporate, sum(user_info.bytes) volume from user_info, subscriber_info  where user_info.sub_id = subscriber_info.sub_id AND subscriber_info.corporate = '44' group by corporate, hour( from_unixtime( user_info.timestamp_id / 1000 ) );
CREATE PROCEDURE Q12 AS select hour( from_unixtime(user_info.timestamp_id / 1000  ) ) time_hour, minute( from_unixtime( user_info.timestamp_id / 1000 ) ) time_minute, subscriber_info.corporate corporate, sum(user_info.bytes) volume from user_info, subscriber_info where user_info.sub_id = subscriber_info.sub_id AND subscriber_info.corporate = '44' group by corporate, hour( from_unixtime(user_info.timestamp_id / 1000  ) ), minute( from_unixtime( user_info.timestamp_id / 1000 ) );

CREATE PROCEDURE Q13 AS select concat( handset_info.manufacturer, ' ', handset_info.model ) model, count( model ) model_count, sum( bytes) volume  from (select handset_id, sum( bytes) bytes from user_info group by handset_id) user_info, handset_info  where user_info.handset_id = handset_info.handset_id  group by concat( handset_info.manufacturer, ' ', handset_info.model )  order by volume, model_count desc limit 20;

CREATE PROCEDURE Q14 AS select * from user_info where sub_id = 213031002743386;
CREATE PROCEDURE Q15 AS select hour( from_unixtime( user_info.timestamp_id / 1000 ) ) time_hour from user_info GROUP BY hour( from_unixtime( user_info.timestamp_id / 1000 ));
CREATE PROCEDURE Q16 AS SELECT HOUR( from_unixtime( user_info.timestamp_id / 1000  ) ) time_hour, minute( from_unixtime( user_info.timestamp_id / 1000  ) ) time_minute, user_info.sub_id sub_id, subscriber_info.userlabel name, sum( user_info.bytes ) volume from user_info, subscriber_info WHERE user_info.sub_id = subscriber_info.sub_id AND user_info.sub_id = 213031002743386 AND user_info.timestamp_id >= 1396332000122 AND user_info.timestamp_id <= 1396335599360 GROUP BY HOUR( from_unixtime( user_info.timestamp_id / 1000  ) ),  minute( from_unixtime( user_info.timestamp_id / 1000  )), sub_id, subscriber_info.userlabel ORDER BY time_hour, time_minute LIMIT 60;
CREATE PROCEDURE Q17 AS select application_id, sub_id from user_info where sub_id = 213031002743386 group by sub_id, application_id limit 10;
CREATE PROCEDURE Q18 AS select coalesce( t2.userlabel, 'UNKNOWN') user_name, t1.sub_id user_id, coalesce( t3.userlabel, 'UNKNOWN') app_name, t1.application_id app_id, sum( t1.bytes ) volume  from user_info t1 LEFT OUTER JOIN subscriber_info t2 ON t1.sub_id = t2.sub_id  LEFT OUTER JOIN application_info t3 ON t1.application_id = t3.application_id where  t1.sub_id = 213031002743386 group by t1.application_id, t1.sub_id, coalesce( t3.userlabel, 'UNKNOWN'), coalesce( t2.userlabel, 'UNKNOWN') order by volume desc limit 10;

-- rewrite with subquery
CREATE PROCEDURE Q19 AS select coalesce( t2.userlabel, 'UNKNOWN') user_name, t1.sub_id user_id,  coalesce( t3.userlabel, 'UNKNOWN') app_name, t1.application_id app_id, sum( t1.sum_bytes ) volume  from (select sub_id, application_id, sum(bytes) sum_bytes from user_info group by sub_id, application_id ) t1 LEFT OUTER JOIN subscriber_info t2 ON t1.sub_id = t2.sub_id  LEFT OUTER JOIN application_info t3 ON t1.application_id = t3.application_id group by t1.application_id, coalesce( t3.userlabel, 'UNKNOWN'), t1.sub_id, coalesce( t2.userlabel,'UNKNOWN') order by volume desc limit 10;

-- better with all the optimizations (ordered subquery, limit push down)
-- CREATE PROCEDURE Q19_rewrite_2 AS select coalesce( t2.userlabel, 'UNKNOWN') user_name, t1.sub_id user_id,  coalesce( t3.userlabel, 'UNKNOWN') app_name, t1.application_id app_id, sum( t1.sum_bytes ) volume  from (select sub_id, application_id, sum(bytes) sum_bytes from user_info group by sub_id, application_id order by sum_bytes desc) t1 LEFT OUTER JOIN subscriber_info t2 ON t1.sub_id = t2.sub_id  LEFT OUTER JOIN application_info t3 ON t1.application_id = t3.application_id group by t1.application_id, coalesce( t3.userlabel, 'UNKNOWN'), t1.sub_id, coalesce( t2.userlabel,'UNKNOWN') order by volume desc limit 10;
-- CREATE PROCEDURE Q19_original AS select  coalesce( t2.userlabel, 'UNKNOWN' ) user_name, t1.sub_id user_id,  coalesce( t3.userlabel, 'UNKNOWN' ) app_name, t1.application_id app_id,  sum( t1.bytes ) volume from      user_info t1  LEFT OUTER JOIN subscriber_info t2 ON t1.sub_id = t2.sub_id LEFT OUTER JOIN application_info t3 ON t1.application_id = t3.application_id group by t1.application_id, t1.sub_id, t3.userlabel, t2.userlabel order by volume desc limit 10;

CREATE PROCEDURE Q20 AS SELECT hour( from_unixtime( user_info.timestamp_id / 1000 ) ) time_hour, minute( from_unixtime( user_info.timestamp_id / 1000 ) ) time_minute,  coalesce( handset_info.os, 'UNKNOWN') os,  sum( user_info.bytes ) volume  from user_info  LEFT OUTER JOIN handset_info handset_info ON user_info.handset_id = handset_info.handset_id WHERE user_info.timestamp_id >= 1396332000122 AND user_info.timestamp_id <= 1396335599360 GROUP BY hour( from_unixtime( user_info.timestamp_id / 1000 ) ), minute( from_unixtime( user_info.timestamp_id / 1000 ) ), coalesce( handset_info.os, 'UNKNOWN') ORDER BY time_hour, time_minute LIMIT 60;



