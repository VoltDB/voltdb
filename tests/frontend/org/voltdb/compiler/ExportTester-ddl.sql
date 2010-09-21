CREATE TABLE A (
  A_CLIENT INTEGER NOT NULL,
  A_ID INTEGER DEFAULT '0' NOT NULL,
  A_VAL INTEGER,
  PRIMARY KEY (A_CLIENT, A_ID)
);

CREATE TABLE B (
  B_CLIENT INTEGER NOT NULL,
  B_ID INTEGER DEFAULT '0' NOT NULL,
  B_VAL INTEGER,
  PRIMARY KEY (B_ID, B_CLIENT)
);

CREATE TABLE C (
  C_CLIENT INTEGER NOT NULL,
  C_ID INTEGER DEFAULT '0' NOT NULL,
  C_VAL INTEGER,
  PRIMARY KEY (C_ID, C_CLIENT)
);

CREATE TABLE D (
  D_CLIENT INTEGER NOT NULL,
  D_ID INTEGER DEFAULT '0' NOT NULL,
  D_VAL INTEGER,
  PRIMARY KEY (D_ID, D_CLIENT)
);

create table e (
  e_client integer not null,
  e_id integer default '0' not null,
  e_val integer,
  primary key (e_id, e_client)
);

create table f (
  f_client integer not null,
  f_id integer default '0' not null,
  f_val integer,
  primary key (f_id, f_client)
);
