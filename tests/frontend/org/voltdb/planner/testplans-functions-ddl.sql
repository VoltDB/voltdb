CREATE TABLE ENG913 (
    name varchar(32),
    city varchar(32)
);


CREATE TABLE bit (
    TINYINT_type  TINYINT,
    INTEGER_TYPE INTEGER,
    BIGINT_TYPE BIGINT,
    FLOAT_TYPE FLOAT,
    VARCHAR_TYPE VARCHAR(60)
);

CREATE INDEX bit_BITAND_IDX ON bit ( bitand(bigint_type, 3) );
CREATE INDEX bit_BITOR_IDX  ON bit ( bitor(bigint_type, 3) );
CREATE INDEX bit_BITXOR_IDX  ON bit ( bitxor(bigint_type, 3) );

CREATE TABLE ENG10749 (
    TIME TIMESTAMP
);
