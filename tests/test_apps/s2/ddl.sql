
load classes procs.jar;

file -inlinebatch EOB

create table points (
       id bigint not null primary key,
       name varchar(64),
       pt point not null,
       cellid bigint not null
);

create index pts_cellid_idx on points(cellid);

create table polygons (
       id bigint not null primary key,
       name varchar(64)
);

create table cellid_poly_map (
       cellid bigint not null,
       polyid bigint not null
);
create index cpm_cellid_idx on cellid_poly_map(cellid);
create index cpm_polyid_idx on cellid_poly_map(polyid);

create procedure from class s2demo.InsertPoint;
create procedure from class s2demo.InsertPolygon;
create procedure from class s2demo.FindContainedPoints;
create procedure from class s2demo.FindIntersectingPolygons;

EOB
