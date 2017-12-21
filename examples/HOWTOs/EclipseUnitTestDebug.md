# Using Eclipse to develop, unit test, and debug VoltDB stored procedures and java clients

This describes a step by step procedure for setting up an Eclipse project with a simple test application that allows unit testing and debugging of stored procedure and client code.

## Prerequisites.
* You have installed a Java 8 JDK
* You have VoltDB:
  * Enterprise or Pro Edition: [Download](https://www.voltdb.com/download) the voltdb-ent-version.tar.gz file.
  * Community Edition:
    * follow wiki instructions on [Building VoltDB](https://github.com/VoltDB/voltdb/wiki/Building-VoltDB), then:
    * run "ant dist" to build ./obj/release/voltdb-<version>.tar.gz
* You have followed [*Using VoltDB:* Chapter 2. Installing VoltDB](https://docs.voltdb.com/UsingVoltDB/ChapGetStarted.php) to install the voltdb[-ent]-version.tar.gz file.
* You have added the voltdb[-ent]-version/bin directory to your PATH
* You have installed Eclipse and set up a workspace

In order to run unit tests or debug stored procedures, it is necessary to run an instance of the VoltDB database within a java process.  In v7.2, code was added to Voltdb to do this.  If you are using v7.1 or earlier, you can load equivalent code from the app-debug-and-test repository.  Follow the steps below to download this repository and build a jar containing these two classes, which you will add to your Eclipse project later.  You can run these commands in any directory you wish.

    git clone https://github.com/VoltDB/app-debug-and-test.git
    cd app-debug-and-test
    ./compile_utils.sh

Check that VoltDBProcedureTestUtils.jar was built in the app-debug-and-test directory.


## Create a new project in Eclipse and configure it for VoltDB.
* Choose File / New / Java Project from the Eclipse menu.
* On the "Create a Java Project" dialog:
  * Project name: provide a name, e.g. TEST.
  * Click "Configure JREs"
  * Select "Java SE 8 [1.8.0_version]", then click "Duplicate"
  * On the JRE Definition dialog:
    * JRE name: add "(for VoltDB application)" as a suffix
    * Default VM arguments: "-ea -Xmx1g"
    * Click "OK"
  * JRE: select "Use a project specific JRE:" and select "Java SE 8 [1.8.0_version] (for VoltDB application)"
  * Click "Finish"


###  Configure the Build Path
VoltDB stored procedures depend on loading a voltdb[-ent]-<version>.jar library jar file found in the "voltdb" folder of your VoltDB distribution.

If you are using v7.1 or earlier, in order to write a unit test that creates an in-process VoltDB instance, you also need the VoltDBProcedureTestUtils.jar file built earlier, as well as the third-party libraries found in the "lib" folder of your VoltDB distribution.

* Right click the "TEST" project and select "Build Path" and then "Configure Build Path...".
* Select the Libraries tab.
* Expand the "JRE System Library..." item in the build path tree
  * Select the "Native library location" item.
  * Click "Edit..." on the right side.
    * In the "Native Library Folder Configuration" dialog:
    * click "External Folder..."
    * Select the "voltdb" folder from your installed VoltDB distribution, then click "Open"
    * Click "OK" on the dialog
* Click "Add External JARs..." and select all jar files in the VoltDB distribution "lib" folder.
* Click "Add External JARs..." and select the voltdb (not client) jar file the VoltDB distribution "voltdb" folder.
* Click "Add External JARs..." and select the app-debug-and-test/VoltDBProcedureTestUtils.jar file you built eariler (only if you are using v7.1 or earlier)
* Click "Add External JARs..." and select the app-debug-and-test/lib/junit-version.jar file.  Optionally, you could use another junit-version.jar file if you already have one.
* Click "OK"

### Add a simple log4j configuration file
* Select the "src" folder
* Choose File / New / File
  * File Name: log4j.xml
  * Click "Finish"

Open the log4j.xml file in the editor.  Then on the bottom of the editor, click on the "Source" tab.  Then paste the following into the editor:

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">
<log4j:configuration xmlns:log4j="http://jakarta.apache.org/log4j/">

    <appender name="console" class="org.apache.log4j.ConsoleAppender">
        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern" value="%d   %-5p [%t] %c: %m%n"/>
        </layout>
    </appender>

    <root>
        <priority value="info" />
        <appender-ref ref="console" />
    </root>

</log4j:configuration>

```

### Add a license file if needed
If using VoltDB Enterprise or Pro Edition, you will need to add a license.xml file to your project.  For Community Edition, this is not necessary.  Later on, when creating the JUnit TestCase class, you will need to reference this file.

Copy your purchased license file into the TEST project folder.  Or, to use the trial license, copy the license.xml file provided in your VoltDB kit under the "voltdb" subfolder into the TEST project folder.  Then use the File / Refresh menu item (or press F5) and the file should appear in the project.


## Create a simple schema
* Choose File / New / File from the menu
  * File name: DDL.sql
  * Click "Finish"

Open the DDL.sql file in the editor and paste in the following:

```sql
CREATE TABLE foo (
  id              INTEGER NOT NULL,
  val             VARCHAR(15),
  last_updated    TIMESTAMP,
  PRIMARY KEY (id)
);
PARTITION TABLE foo ON COLUMN id;
CREATE PROCEDURE PARTITION ON TABLE foo COLUMN id FROM CLASS procedures.UpdateFoo;
```

## Create a stored procedure
* Choose File / New / Package from the menu.
  * Name: procedures
  * Click "Finish"
* Select the "procedures" package and choose New / Class from the context menu.
  * Name: UpdateFoo
  * Superclass: org.voltdb.VoltProcedure
  * Click "Finish"

Open the UpdateFoo.java file in the editor and paste in the following:

```java
package procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltProcedure.VoltAbortException;

public class UpdateFoo extends VoltProcedure {

    public final SQLStmt updateFoo = new SQLStmt(
        "UPDATE foo SET last_updated = NOW, val = ? WHERE id = ?;");
    public final SQLStmt insertFoo = new SQLStmt(
        "INSERT INTO foo VALUES (?,?,NOW);");

    public VoltTable[] run(int id, String val) throws VoltAbortException {
        voltQueueSQL(updateFoo, val, id);
        VoltTable[] results = voltExecuteSQL();
        if (results[0].asScalarLong() == 0) {
            voltQueueSQL(insertFoo,id,val);
            results = voltExecuteSQL();
        }
        return results;
    }
}

```


## Create a test class
* Choose File / New / Package from the menu.
  * Name: tests
  * Click "Finish"
* Select the package and choose New / Class from the context menu.
  * Name: BasicTest
  * Superclass: junit.framework.TestCase
  * Press Finish to create and open the new class.

Open the BasicTest.java file in the editor and paste in the following:

```java
package tests;

import java.util.Date;
import junit.framework.TestCase;
import org.voltdb.InProcessVoltDBServer;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

public class BasicTest extends TestCase {

    public void testProcedureReturn() throws Exception {

        // Create an in-process VoltDB server instance
        InProcessVoltDBServer volt = new InProcessVoltDBServer();

        // If using Enterprise or Pro Edition, a license is required.
        // If using Community Edition, comment out the following line.
        volt.configPathToLicense("./license.xml");

        // Start the database
        volt.start();

        // Load the schema
        volt.runDDLFromPath("./ddl.sql");

        // Create a client to communicate with the database
        Client client = volt.getClient();

        // TESTS...

        // insert a row using a default procedure
        int id = 1;
        String val = "Hello VoltDB";
        Date initialDate = new Date();
        ClientResponse response = client.callProcedure("FOO.insert",id,val,initialDate);
        assertEquals(response.getStatus(),ClientResponse.SUCCESS);

        // try inserting the same row, expect a unique constraint violation
        try {
            response = client.callProcedure("FOO.insert",id,val,initialDate);
        } catch (ProcCallException e) {
        }

        // call the UpdateFoo procedure
        val = "Hello again";
        response = client.callProcedure("UpdateFoo", id, val);
        assertEquals(response.getStatus(), ClientResponse.SUCCESS);
        // check that one row was updated
        assertEquals(response.getResults()[0].asScalarLong(), 1);

        // select the row and check the values
        response = client.callProcedure("FOO.select", id);
        VoltTable t = response.getResults()[0];
        assertEquals(t.getRowCount(),1);
        t.advanceRow();
        long lastUpdatedMicros = t.getTimestampAsLong("LAST_UPDATED");
        long initialDateMicros = initialDate.getTime()*1000;
        assertTrue(lastUpdatedMicros > initialDateMicros);
        String latestVal = t.getString("VAL");
        assertEquals(latestVal,val);


        volt.shutdown();
    }

}

```

## Run the JUnit Test
Select the BasicTest.java class, and click the green "Run" button from the toolbar, or right-click and select Run As... > JUnit Test.  The test should complete successfully.


## Debugging the test client and stored procedure.
At this point you can launch the BasicTest class. You can freely set breakpoints in either the client code or the stored procedure since they run in the same process.
