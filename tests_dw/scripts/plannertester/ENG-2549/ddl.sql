create table l 
( 
    id bigint NOT NULL, 
    lname varchar(32) NOT NULL, 
    a tinyint NOT NULL, 
    b tinyint NOT NULL, 
    CONSTRAINT PK_LOG PRIMARY KEY ( lname, id ) 
); 
CREATE INDEX IDX_B on l (lname, b, id); 


partition table l on column id;

create procedure orderone AS select * from l where lname=? and b=0 order by id asc limit ?;
create procedure orderthree AS select * from l where lname=? and b=0 order by lname, b, id asc limit ?;
