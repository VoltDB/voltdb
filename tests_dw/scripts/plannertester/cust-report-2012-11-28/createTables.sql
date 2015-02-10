
create table tuid (
tuid bigint not null,
clientID integer default 0 not null,
constraint tree_tuid_pk primary key( tuid, clientID )
);

PARTITION TABLE tuid ON COLUMN tuid;
