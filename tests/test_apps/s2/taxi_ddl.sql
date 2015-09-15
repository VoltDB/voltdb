
load classes procs.jar;

file -inlinebatch EOB

create table taxis (
    id              bigint not null primary key,
    location        point not null,
    cellid          bigint not null,
    geoid           biging not null
);

create index taxis_cellid_idx on taxis(cellid);

create table cities (
    id              bigint not null primary key,
    name            varchar not null,
    location        point not null,
    cellid          bigint not null,
    geoid           biging not null
);

create index cities_cellid_idx on cities(cellid);

create table counties (
    id bigint       not null primary key,
    name            varchar(64) not null,
    fips            integer not null,
    boundary        varbinary(16004) not null
);

create table states (
    id              bigint not null primary key,
    name            varchar(64) not null,
    fips            integer not null,
    boundary        varbinary(16004) not null
);
        
create table cellid_county_map (
    cellid          bigint not null,
    countyid        bigint not null
);
create index ccm_cellid_idx on cellid_county_map(cellid);
create index ccm_countyid_idx on cellid_county_map(countyid);

create table cellid_state_map (
    cellid          bigint not null,
    stateid         bigint not null
);
create index csm_cellid_idx on cellid_poly_map(cellid);
create index csm_statid_idx on cellid_poly_map(stateid);


create procedure from class s2demo.InsertPoint;
create procedure from class s2demo.InsertPolygon;
create procedure from class s2demo.FindContainedPoints;
create procedure from class s2demo.FindIntersectingPolygons;
create procedure from class s2demo.FindNearestPoints;

EOB
