create table CUSTOMERS (NAME varchar(20) not null,
                        ID bigint not null,
                        primary key(NAME));
partition table CUSTOMERS on column NAME;

create table PARTS (NAME varchar(20) not null,
                    PARTNUM bigint not null,
                    AVAILABLE int not null,
                    primary key(NAME));
partition table PARTS on column NAME;

create table ORDERS (ORDERID bigint not null,
                     CUSTID bigint not null,
                     PARTNUM bigint not null,
                     QUANTITY int not null,
                     primary key(ORDERID));
partition table ORDERS on column ORDERID;

create compound procedure from class OrderProc;
