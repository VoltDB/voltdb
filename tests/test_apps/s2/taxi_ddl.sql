
load classes procs.jar;

file -inlinebatch EOB

create table taxis (
    id              bigint not null primary key,
    location        point not null,
    cellid          bigint not null
);

create index taxis_cellid_idx on taxis(cellid);
partition table taxis on column id;

create table cities (
    id              bigint not null primary key,
    name            varchar not null,
    location        point not null,
    cellid          bigint not null
);

create index cities_cellid_idx on cities(cellid);

create table region (
    id bigint       not null primary key,
    name            varchar(64) not null,
    -- This is 0 for counties, 1 for states.
    kind            integer not null,
    boundary        varbinary(16004) not null
);

create table cellid_region_map (
    cellid          bigint not null,
    regionid        bigint not null
);
create index ccm_cellid_idx on cellid_region_map(cellid);
create index ccm_regionid_idx on cellid_region_map(regionid);

-- Insert a new city in the city table.
-- Cities are constant.
create procedure from class iwdemo.InsertCity;
create procedure
  partition on table taxis column id parameter 0
  from class iwdemo.TaxisPerCity;

create procedure selectTaxiLocations as
  select location from taxis order by id;

-- Insert a new region in the region table
-- Regions are constant.
-- create procedure from class iwdemo.InsertRegion;
-- Find all the regions containing a particular taxi.
-- create procedure from class iwdemo.FindTaxi;
-- Find all the counties in a given state.
-- create procedure from class iwdemo.FindCountiesInState;
-- Find the nearest points.
-- create procedure from class iwdemo.FindNearestPoints;

EOB
