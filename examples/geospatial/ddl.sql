file -inlinebatch EOF
-- With population of at least 100,000
create table us_cities (
       id integer not null primary key,
       name varchar(128) not null,
       state varchar(128) not null,
       population bigint not null,
       geo_json varchar(128) not null
);

create table us_states (
       id integer not null primary key,
       postalcode varchar(2) not null,
       name varchar(128) not null,
       geo_json varchar(500000 bytes) not null
);

create table us_counties (
       id integer not null,
       name varchar(128) not null,
       state_id integer not null,
       geo_json varchar(500000 bytes) not null
);

-- radius of earth is 3959 miles

create procedure largestStates as
       select name, geo_area(geo_json) * 3959*3959 as area_in_miles
       from us_states
       order by area_in_miles desc
       limit 5;

create procedure mostDistantCities as
       select cities1.name,
              cities2.name,
              geo_distance(cities1.geo_json, cities2.geo_json)
                * 3959 as distance
       from us_cities as cities1, us_cities as cities2
       where cities1.id < cities2.id
       order by distance desc
       limit 5;

create procedure statesWithMostPolygons as
       select us_states.name,
              geo_num_polygons(us_states.geo_json) as num_polys
       from us_states
       order by num_polys desc
       limit 5;

create procedure statesWithFewestPoints as
       select us_states.name,
              geo_num_points(us_states.geo_json) as num_points
       from us_states
       order by num_points asc
       limit 5;

-- geo-spatial joins!

create procedure citiesInState as
       select us_cities.name, us_cities.population
       from us_states
         inner join us_cities
         on geo_within(us_cities.geo_json, us_states.geo_json) = 1
       where us_states.name = ?
       order by us_cities.population desc;


create procedure citiesPerCounty as
       select us_counties.name as county_name,
              count(*) as cnt
       from us_counties
         inner join us_states
           on us_counties.state_id = us_states.id
         inner join us_cities
         on geo_within(us_cities.geo_json, us_counties.geo_json) = 1
       where us_states.name = ?
       group by county_name
       order by cnt desc;



----------------------------------------


create procedure numCitiesPerState as
       select us_states.name, count(*)
       from us_states
         inner join us_cities
         on geo_within(us_cities.geo_json, us_states.geo_json) = 1
       group by us_states.name
       order by count(*) desc;

create procedure populationPerState as
       select us_states.name as state,
              sum(us_cities.population) as population
       from us_states
         inner join us_cities
         on geo_within(us_cities.geo_json, us_states.geo_json) = 1
       group by us_states.name
       order by population desc;

create procedure averagePointsPerState as
       select avg(geo_num_points(us_states.geo_json)) as avg_pts
       from us_states;

EOF
