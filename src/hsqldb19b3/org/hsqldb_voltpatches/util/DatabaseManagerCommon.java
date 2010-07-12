/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

// sqlbob@users 20020401 - patch 1.7.0 by sqlbob (RMP) - enhancements
// sqlbob@users 20020407 - patch 1.7.0 - reengineering
// nickferguson@users 20021005 - patch 1.7.1 - enhancements
// fredt@users 20021012 - patch 1.7.1 - changes to test database DDL
// weconsultants@users 20041116 - patch 1.8.0 - in 'TestHelp' added 'IF EXISTS for both DROPS.
//                                Now catching the execption that was never caught before.

/**
 * Common code in Swing and AWT versions of DatabaseManager
 *
 * New class based on Hypersonic original
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @version 1.8.0
 * @since 1.7.0
 */
class DatabaseManagerCommon {

    private static Random rRandom    = new Random(100);
    static String[]       selectHelp = {
        "SELECT * FROM ",

        "SELECT [LIMIT n m] [DISTINCT] \n"
        + "{ selectExpression | table.* | * } [, ... ] \n"
        + "[INTO [CACHED|TEMP|TEXT] newTable] \n" + "FROM tableList \n"
        + "[WHERE Expression] \n"
        + "[ORDER BY selectExpression [{ASC | DESC}] [, ...] ] \n"
        + "[GROUP BY Expression [, ...] ] \n"                             //
        + "[UNION [ALL] selectStatement]"
    };
    static String[] insertHelp = {
        "INSERT INTO ",
        "INSERT INTO table [ (column [,...] ) ] \n"
        + "{ VALUES(Expression [,...]) | SelectStatement }"
    };
    static String[] updateHelp = {
        "UPDATE ",
        "UPDATE table SET column = Expression [, ...] \n"
        + "[WHERE Expression]"
    };
    static String[] deleteHelp      = {
        "DELETE FROM ", "DELETE FROM table [WHERE Expression]"
    };
    static String[] createTableHelp = {
        "CREATE TABLE ",
        "CREATE [TEMP] [CACHED|MEMORY|TEXT] TABLE name \n"
        + "( columnDefinition [, ...] ) \n\n" + "columnDefinition: \n"
        + "column DataType [ [NOT] NULL] [PRIMARY KEY] \n" + "DataType: \n"
        + "{ INTEGER | DOUBLE | VARCHAR | DATE | TIME |... }"
    };
    static String[] dropTableHelp   = {
        "DROP TABLE ", "DROP TABLE table"
    };
    static String[] createIndexHelp = {
        "CREATE INDEX ",
        "CREATE [UNIQUE] INDEX index ON \n" + "table (column [, ...])"
    };
    static String[] dropIndexHelp  = {
        "DROP INDEX ", "DROP INDEX table.index"
    };
    static String[] checkpointHelp = {
        "CHECKPOINT", "(HSQLDB SQL only)"
    };
    static String[] scriptHelp     = {
        "SCRIPT", "SCRIPT ['file']\n\n" + "(HSQLDB SQL only)"
    };
    static String[] shutdownHelp   = {
        "SHUTDOWN",
        "SHUTDOWN [COMPACT|IMMEDIATELY|SCRIPT]\n\n" + "(HSQLDB SQL only)"
    };
    static String[] setHelp = {
        "SET ",

        "SET AUTOCOMMIT { TRUE | FALSE }\n"
        + "SET DATABASE COLLATION \"<collationname>\"\n"
        + "SET CHECKPOINT DEFRAG <size>\n"
        + "SET IGNORECASE { TRUE | FALSE }\n"
        + "SET INITIAL SCHEMA <schemaname>\n"                             //
        + "SET LOGSIZE <size>\n"                                          //
        + "SET MAXROWS maxrows\n"                                         //
        + "SET PASSWORD <password>\n"                                     //
        + "SET PROPERTY \"<propname>\" <propvalue>\n"
        + "SET READONLY { TRUE | FALSE }\n"
        + "SET DATABASE REFERENTIAL_INTEGRITY { TRUE | FALSE }\n"
        + "SET SCHEMA <schemaname>\n"
        + "SET DATABASE SCRIPTFORMAT { TEXT | BINARY | COMPRESSED }\n"
        + "SET TABLE <tablename> READONLY { TRUE | FALSE }\n"
        + "SET TABLE <tablename> SOURCE \"<file>\" [DESC]\n"
        + "SET WRITE_DELAY { TRUE | FALSE | <seconds> | <ms> MILLIS }"    //
        + "\n\n"                                                          //
        + "(HSQLDB SQL only)"
    };
    static String[] testHelp = {
        "-->>>TEST<<<-- ;\n" + "--#1000;\n" + "DROP TABLE Test IF EXISTS;\n"
        + "CREATE TABLE Test(\n" + "  Id INTEGER PRIMARY KEY,\n"
        + "  FirstName VARCHAR(20),\n" + "  Name VARCHAR(50),\n"
        + "  ZIP INTEGER) ;\n" + "INSERT INTO Test \n"
        + "  VALUES(#,'Julia','Peterson-Clancy',#) ;\n"
        + "UPDATE Test SET Name='Hans' WHERE Id=# ;\n"
        + "SELECT * FROM Test WHERE Id=# ;\n"
        + "DELETE FROM Test WHERE Id=# ;\n" + "DROP TABLE Test IF EXISTS;",
        "This test script is parsed by the DatabaseManager\n"
        + "It may be changed manually. Rules:\n"
        + "- it must start with -->>>TEST<<<--.\n"
        + "- each line must end with ';' (no spaces after)\n"
        + "- lines starting with -- are comments\n"
        + "- lines starting with --#<count> means set new count\n"
    };
    static String[] testDataSql = {
        "SELECT * FROM Product",                                          //
        "SELECT * FROM Invoice",                                          //
        "SELECT * FROM Item",
        "SELECT * FROM Customer a INNER JOIN Invoice i ON a.ID=i.CustomerID",
        "SELECT * FROM Customer a LEFT OUTER JOIN Invoice i ON a.ID=i.CustomerID",
        "SELECT * FROM Invoice d INNER JOIN Item i ON d.ID=i.InvoiceID",
        "SELECT * FROM Customer WHERE Street LIKE '1%' ORDER BY Lastname",
        "SELECT a.id, a.firstname, a.lastname, count(i.Total) \"COUNT\", "
        + "COALESCE(sum(i.Total), 0) \"TOTAL\", COALESCE(AVG(i.Total),0) \"AVG\" FROM Customer a "
        + "LEFT OUTER JOIN Invoice i ON a.ID=i.CustomerID GROUP BY a.id, a.firstname, a.lastname"
    };

    static String random(String[] s) {
        return s[random(s.length)];
    }

    static int random(int i) {

        i = rRandom.nextInt() % i;

        return i < 0 ? -i
                     : i;
    }

    static void createTestTables(Statement sStatement) {

        String[] demo = {
            "DROP TABLE Item IF EXISTS;", "DROP TABLE Invoice IF EXISTS;",
            "DROP TABLE Product IF EXISTS;", "DROP TABLE Customer IF EXISTS;",
            "CREATE TABLE Customer(ID INTEGER PRIMARY KEY,FirstName VARCHAR(20),"
            + "LastName VARCHAR(20),Street VARCHAR(20),City VARCHAR(20));",
            "CREATE TABLE Product(ID INTEGER PRIMARY KEY,Name VARCHAR(20),"
            + "Price DECIMAL(10,2));",
            "CREATE TABLE Invoice(ID INTEGER PRIMARY KEY,CustomerID INTEGER,"
            + "Total DECIMAL(10,2), FOREIGN KEY (CustomerId) "
            + "REFERENCES Customer(ID) ON DELETE CASCADE);",
            "CREATE TABLE Item(InvoiceID INTEGER,Item INTEGER,"
            + "ProductID INTEGER,Quantity INTEGER,Cost DECIMAL(10,2),"
            + "PRIMARY KEY(InvoiceID,Item), "
            + "FOREIGN KEY (InvoiceId) REFERENCES "
            + "Invoice (ID) ON DELETE CASCADE, FOREIGN KEY (ProductId) "
            + "REFERENCES Product(ID) ON DELETE CASCADE);"
        };

        for (int i = 0; i < demo.length; i++) {

            // drop table may fail
            try {
                sStatement.execute(demo[i]);
            } catch (SQLException e) {
                ;
            }
        }
    }

    static String createTestData(Statement sStatement) throws SQLException {

        String[] name = {
            "White", "Karsen", "Smith", "Ringer", "May", "King", "Fuller",
            "Miller", "Ott", "Sommer", "Schneider", "Steel", "Peterson",
            "Heiniger", "Clancy"
        };
        String[] firstname = {
            "Mary", "James", "Anne", "George", "Sylvia", "Robert", "Janet",
            "Michael", "Andrew", "Bill", "Susanne", "Laura", "Bob", "Julia",
            "John"
        };
        String[] street = {
            "Upland Pl.", "College Av.", "- 20th Ave.", "Seventh Av."
        };
        String[] city   = {
            "New York", "Dallas", "Boston", "Chicago", "Seattle",
            "San Francisco", "Berne", "Oslo", "Paris", "Lyon", "Palo Alto",
            "Olten"
        };
        String[] product = {
            "Iron", "Ice Tea", "Clock", "Chair", "Telephone", "Shoe"
        };
        int      max     = 50;

        for (int i = 0; i < max; i++) {
            sStatement.execute("INSERT INTO Customer VALUES(" + i + ",'"
                               + random(firstname) + "','" + random(name)
                               + "','" + random(554) + " " + random(street)
                               + "','" + random(city) + "')");
            sStatement.execute("INSERT INTO Product VALUES(" + i + ",'"
                               + random(product) + " " + random(product)
                               + "'," + (20 + 2 * random(120)) + ")");
        }

        for (int i = 0; i < max; i++) {
            sStatement.execute("INSERT INTO Invoice VALUES(" + i + ","
                               + random(max) + ",0.0)");

            for (int j = random(20) + 2; j >= 0; j--) {
                sStatement.execute("INSERT INTO Item VALUES(" + i + "," + j
                                   + "," + random(max) + ","
                                   + (1 + random(24)) + ",1.5)");
            }
        }

        sStatement.execute("UPDATE Product SET Price=ROUND(Price*.1,2)");
        sStatement.execute(
            "UPDATE Item SET Cost=Cost*"
            + "(SELECT Price FROM Product prod WHERE ProductID=prod.ID)");
        sStatement.execute(
            "UPDATE Invoice SET Total=(SELECT SUM(Cost*"
            + "Quantity) FROM Item WHERE InvoiceID=Invoice.ID)");

        return ("SELECT * FROM Customer");
    }

    /**
     * Redid this file to remove sizing requirements and to make it faster
     * Speeded it up 10 fold.
     *
     * @param file
     */
    static String readFile(String file) {

        try {
            FileReader     reader = new FileReader(file);
            BufferedReader read   = new BufferedReader(reader);
            StringBuffer   b      = new StringBuffer();
            String         s      = null;
            int            count  = 0;

            while ((s = read.readLine()) != null) {
                count++;

                b.append(s);
                b.append('\n');
            }

            read.close();
            reader.close();

            return b.toString();
        } catch (IOException e) {
            return e.getMessage();
        }
    }

    static void writeFile(String file, String text) {

        try {
            FileWriter write = new FileWriter(file);

            write.write(text.toCharArray());
            write.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @throws SQLException
     */
    static long testStatement(Statement sStatement, String sql,
                              int max) throws SQLException {

        long start = System.currentTimeMillis();

        if (sql.indexOf('#') == -1) {
            max = 1;
        }

        for (int i = 0; i < max; i++) {
            String s = sql;

            while (true) {
                int j = s.indexOf("#r#");

                if (j == -1) {
                    break;
                }

                s = s.substring(0, j) + ((int) (Math.random() * i))
                    + s.substring(j + 3);
            }

            while (true) {
                int j = s.indexOf('#');

                if (j == -1) {
                    break;
                }

                s = s.substring(0, j) + i + s.substring(j + 1);
            }

            sStatement.execute(s);
        }

        return (System.currentTimeMillis() - start);
    }

    private DatabaseManagerCommon() {}
}
