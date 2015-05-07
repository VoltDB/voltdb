--
-- The following four tables are used to test insert and select of
-- min, max, 0, NULL and some normal values of each SQL type. These
-- tables are not designed to test any partitioning effects.
--

-- Attribute for ea. supported type: nulls allowed
CREATE TABLE importTable (
 PKEY          BIGINT       NOT NULL,
 A_INTEGER_VALUE     BIGINT,
);

