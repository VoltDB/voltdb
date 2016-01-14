-- Database creation script for the simple VoltDB schema, ported from the original PostgreSQL schema "pgsimple_schema_0.6"
-- found here: https://github.com/openstreetmap/osmosis/blob/master/package/script/pgsimple_schema_0.6.sql
-- the schema is translated as literally as possible to highlight the similiarities between the implementations.
-- This can be enhanced with several VoltDB optimizations, such as creating the geometry columns inside the initial
-- create table statements, but has been kept as similiar as possible for easy comparison purposes.

-- Drop all tables if they exist.
DROP TABLE actions IF EXISTS;
DROP TABLE users IF EXISTS;
DROP TABLE nodes IF EXISTS;
DROP TABLE node_tags IF EXISTS;
DROP TABLE ways IF EXISTS;
DROP TABLE way_nodes IF EXISTS;
DROP TABLE way_tags IF EXISTS;
DROP TABLE relations IF EXISTS;
DROP TABLE relation_members IF EXISTS;
DROP TABLE relation_tags IF EXISTS;
DROP TABLE schema_info IF EXISTS;

-- Create a table which will contain a single row defining the current schema version.
CREATE TABLE schema_info (
    version integer NOT NULL
);


-- Create a table for users.
CREATE TABLE users (
    id int NOT NULL,
    name varchar(128) NOT NULL
);
PARTITION TABLE users ON COLUMN id;

-- Create a table for nodes.
CREATE TABLE nodes (
    id bigint NOT NULL,
    version int NOT NULL,
    user_id int NOT NULL,
    tstamp timestamp without time zone NOT NULL,
    changeset_id bigint NOT NULL
);
-- Add a postgis point column holding the location of the node.
-- SELECT AddGeometryColumn('nodes', 'geom', 4326, 'POINT', 2);
ALTER TABLE nodes add COLUMN geom GEOGRAPHY_POINT;

PARTITION TABLE nodes on COLUMN id;

-- Create a table for node tags.
CREATE TABLE node_tags (
    node_id bigint NOT NULL,
    k varchar(128) NOT NULL,
    v varchar(256) NOT NULL
);

PARTITION TABLE node_tags ON COLUMN node_id;

-- Create a table for ways.
CREATE TABLE ways (
    id bigint NOT NULL,
    version int NOT NULL,
    user_id int NOT NULL,
    tstamp timestamp without time zone NOT NULL,
    changeset_id bigint NOT NULL
);
PARTITION TABLE ways ON COLUMN id;

ALTER TABLE ways add COLUMN bbox GEOGRAPHY;

-- Create a table for representing way to node relationships.
CREATE TABLE way_nodes (
    way_id bigint NOT NULL,
    node_id bigint NOT NULL,
    sequence_id int NOT NULL
);

PARTITION TABLE way_nodes ON COLUMN way_id;

-- Create a table for way tags.
CREATE TABLE way_tags (
    way_id bigint NOT NULL,
    k varchar(128) NOT NULL,
    v varchar(256)
);

PARTITION TABLE way_tags ON COLUMN way_id;

-- Create a table for relations.
CREATE TABLE relations (
    id bigint NOT NULL,
    version int NOT NULL,
    user_id int NOT NULL,
    tstamp timestamp without time zone NOT NULL,
    changeset_id bigint NOT NULL
);

PARTITION TABLE relations ON COLUMN id;

-- Create a table for representing relation member relationships.
CREATE TABLE relation_members (
    relation_id bigint NOT NULL,
    member_id bigint NOT NULL,
    member_type character(1) NOT NULL,
    member_role varchar(256) NOT NULL,
    sequence_id int NOT NULL
);

PARTITION TABLE relation_members ON COLUMN relation_id;

-- Create a table for relation tags.
CREATE TABLE relation_tags (
    relation_id bigint NOT NULL,
    k varchar(128) NOT NULL,
    v varchar(256) NOT NULL
);

PARTITION TABLE relation_tags ON COLUMN relation_id;

-- Configure the schema version.
-- INSERT INTO schema_info (version) VALUES (5);


-- Add primary keys to tables.
ALTER TABLE schema_info ADD CONSTRAINT pk_schema_info PRIMARY KEY (version);

ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id);

ALTER TABLE nodes ADD CONSTRAINT pk_nodes PRIMARY KEY (id);

ALTER TABLE ways ADD CONSTRAINT pk_ways PRIMARY KEY (id);

ALTER TABLE way_nodes ADD CONSTRAINT pk_way_nodes PRIMARY KEY (way_id, sequence_id);

ALTER TABLE relations ADD CONSTRAINT pk_relations PRIMARY KEY (id);

ALTER TABLE relation_members ADD CONSTRAINT pk_relation_members PRIMARY KEY (relation_id, sequence_id);


-- Add indexes to tables.
CREATE INDEX idx_node_tags_node_id ON node_tags (node_id);
-- this isn't currently supported
-- CREATE INDEX idx_nodes_geom ON nodes (geom);

CREATE INDEX idx_way_tags_way_id ON way_tags (way_id);
CREATE INDEX idx_way_nodes_node_id ON way_nodes (node_id);

CREATE INDEX idx_relation_tags_relation_id ON relation_tags (relation_id);

