
load classes procs.jar;

file -inlinebatch EOB

create table taxis (
    id              bigint not null primary key,
    location        point not null,
    cellid          bigint not null
);

create index taxis_cellid_idx on taxis(cellid);

create table cities (
    id              bigint not null primary key,
    name            varchar not null,
    location        point not null,
    cellid          bigint not null
);

create index cities_cellid_idx on cities(cellid);

create table state (
    id              bigint not null primary key,
    name            varchar not null,
    USPS            varchar not null
);

create table county (
    id              bigint not null primary key,
    name            varchar not null,
    -- This would be a foreign key in the state table.
    state           bigint not null
);

-- Some states and some counties have multiple
-- components.  We keep their boundaries all here.
--
create table region (
    id              bigint not null primary key,
    -- This would be a foreign key in the state
    -- or county table.
    containerid     bigint not null,
    -- This would be the id in the container.
    -- For single connected regions this would be
    -- just 0.  For multiply connected regions
    -- this could be larger.
    componentnum    bigint not null,
    -- This is 0 for counties, 1 for states.
    kind            integer not null,
    boundary        varbinary(120000) not null
);

create table cellid_region_map (
    cellid          bigint not null,
    regionid        bigint not null
);
create index ccm_cellid_idx on cellid_region_map(cellid);
create index ccm_regionid_idx on cellid_region_map(regionid);

-- Upsert a new taxi in the taxi table.
-- We want to be able to update its location.
-- This has to be a shared procedure and not built
-- in because w need to calculate the cell id.
-- create procedure from class iwdemo.UpsertTaxi;
-- Insert a new city in the city table.
-- Cities are constant, so this is an Insert and
-- not an Upsert.  This has to be a shared
-- procedure because we want to make a cellid. 
create procedure from class iwdemo.InsertCity;
-- Insert a new region in the region table
-- Regions are constant.  We want to make a
-- tesselation, so this has to be a region.
create procedure from class iwdemo.InsertRegion;
-- Find all the regions containing a particular taxi.
-- create procedure from class iwdemo.FindTaxi;
-- Find all the counties in a given state.
-- create procedure from class iwdemo.FindCountiesInState;
-- Find the nearest points.
-- create procedure from class iwdemo.FindNearestPoints;

EOB
