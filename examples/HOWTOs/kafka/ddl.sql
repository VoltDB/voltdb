CREATE TABLE Customer (
  CustomerID INTEGER UNIQUE NOT NULL,
  FirstName VARCHAR(15),
  LastName VARCHAR(15),
  PRIMARY KEY(CustomerID)
);

CREATE STREAM Customer_final
  EXPORT TO TARGET test (
   	CustomerID INTEGER NOT NULL,
  	FirstName VARCHAR(15) NOT NULL,
  	LastName VARCHAR(15) NOT NULL
);

-- The above STREAM corresponds to this line in the deployment_k.xml file:
-- <property name="topic.key">Customer_final.test</property>

PARTITION TABLE Customer ON COLUMN CustomerID;

CREATE PROCEDURE add_customer AS INSERT INTO Customer VALUES ?,?,?;

PARTITION PROCEDURE add_customer ON TABLE Customer COLUMN CustomerID;

CREATE PROCEDURE acs AS INSERT INTO Customer_final VALUES ?,?,?;