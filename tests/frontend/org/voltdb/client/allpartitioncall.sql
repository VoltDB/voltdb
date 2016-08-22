CREATE TABLE TABLE_INT_PARTITION
(
  id       			 		bigint     NOT NULL,
  value_string              varchar(50) NOT NULL,
  value_number1  			bigint    NOT NULL,
  value_number2  			bigint    NOT NULL
);

PARTITION TABLE TABLE_INT_PARTITION ON COLUMN value_number1;

CREATE TABLE TABLE_STRING_PARTITION
(
  id       			 		bigint     NOT NULL,
  value_string              varchar(50) NOT NULL,
  value_number1  			bigint    NOT NULL,
  value_number2  			bigint    NOT NULL
);

PARTITION TABLE TABLE_STRING_PARTITION ON COLUMN value_string;