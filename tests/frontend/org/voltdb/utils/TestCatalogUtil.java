/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import junit.framework.TestCase;

import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.ConnectorProperty;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Systemsettings;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TestCatalogDiffs;
import org.voltdb.catalog.User;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.compilereport.ProcedureAnnotation;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.types.ConstraintType;

public class TestCatalogUtil extends TestCase {

    protected Catalog catalog;
    protected Database catalog_db;

    @Override
    protected void setUp() throws Exception {
        catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
        assertNotNull(catalog);
        catalog_db = catalog.getClusters().get("cluster").getDatabases().get("database");
        assertNotNull(catalog_db);
    }

    /**
     *
     */
    public void testGetSortedCatalogItems() {
        for (Table catalog_tbl : catalog_db.getTables()) {
            int last_idx = -1;
            List<Column> columns = CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index");
            assertFalse(columns.isEmpty());
            assertEquals(catalog_tbl.getColumns().size(), columns.size());
            for (Column catalog_col : columns) {
                assertTrue(catalog_col.getIndex() > last_idx);
                last_idx = catalog_col.getIndex();
            }
        }
    }

    /**
     *
     */
    public void testToSchema() {
        String search_str = "";

        // Simple check to make sure things look ok...
        for (Table catalog_tbl : catalog_db.getTables()) {
            StringBuilder sb = new StringBuilder();
            CatalogSchemaTools.toSchema(sb, catalog_tbl, null, false);
            String sql = sb.toString();
            assertTrue(sql.startsWith("CREATE TABLE " + catalog_tbl.getTypeName()));

            // Columns
            for (Column catalog_col : catalog_tbl.getColumns()) {
                assertTrue(sql.indexOf(catalog_col.getTypeName()) != -1);
            }

            // Constraints
            for (Constraint catalog_const : catalog_tbl.getConstraints()) {
                ConstraintType const_type = ConstraintType.get(catalog_const.getType());
                Index catalog_idx = catalog_const.getIndex();
                List<ColumnRef> columns = CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index");

                if (!columns.isEmpty()) {
                    search_str = "";
                    String add = "";
                    for (ColumnRef catalog_colref : columns) {
                        search_str += add + catalog_colref.getColumn().getTypeName();
                        add = ", ";
                    }
                    assertTrue(sql.indexOf(search_str) != -1);
                }

                switch (const_type) {
                    case PRIMARY_KEY:
                        assertTrue(sql.indexOf("PRIMARY KEY") != -1);
                        break;
                    case FOREIGN_KEY:
                        search_str = "REFERENCES " + catalog_const.getForeignkeytable().getTypeName();
                        assertTrue(sql.indexOf(search_str) != -1);
                        break;
                }
            }
        }
    }

    public void testDeploymentHeartbeatConfig()
    {
        final String dep =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <admin-mode port='32323' adminstartup='true'/>" +
            "   <heartbeat timeout='30'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <httpd port='0' >" +
            "       <jsonapi enabled='true'/>" +
            "   </httpd>" +
            "</deployment>";

        // make sure someone can't give us 0 for timeout value
        final String boom =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <admin-mode port='32323' adminstartup='true'/>" +
            "   <heartbeat timeout='0'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <httpd port='0' >" +
            "       <jsonapi enabled='true'/>" +
            "   </httpd>" +
            "</deployment>";

        final File tmpDep = VoltProjectBuilder.writeStringToTempFile(dep);
        final File tmpBoom = VoltProjectBuilder.writeStringToTempFile(boom);

        long crcDep = CatalogUtil.compileDeployment(catalog, tmpDep.getPath(), true, false);

        assertEquals(30, catalog.getClusters().get("cluster").getHeartbeattimeout());

        // This returns -1 on schema violation
        crcDep = CatalogUtil.compileDeployment(catalog, tmpBoom.getPath(), true, false);
        assertEquals(-1, crcDep);
    }

    public void testAutoSnapshotEnabledFlag() throws Exception
    {
        final String depOff =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <snapshot frequency=\"5s\" retain=\"10\" prefix=\"pref2\" enabled=\"false\"/>" +
            "</deployment>";

        final String depOn =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <snapshot frequency=\"5s\" retain=\"10\" prefix=\"pref2\" enabled=\"true\"/>" +
            "</deployment>";

        final File tmpDepOff = VoltProjectBuilder.writeStringToTempFile(depOff);
        CatalogUtil.compileDeployment(catalog, tmpDepOff.getPath(), true, false);
        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");
        assertFalse(db.getSnapshotschedule().get("default").getEnabled());

        setUp();
        final File tmpDepOn = VoltProjectBuilder.writeStringToTempFile(depOn);
        CatalogUtil.compileDeployment(catalog, tmpDepOn.getPath(), true, false);
        db = catalog.getClusters().get("cluster").getDatabases().get("database");
        assertFalse(db.getSnapshotschedule().isEmpty());
        assertTrue(db.getSnapshotschedule().get("default").getEnabled());
        assertEquals(10, db.getSnapshotschedule().get("default").getRetain());
    }

    public void testSecurityEnabledFlag() throws Exception
    {
        final String secOff =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <security enabled=\"false\"/>" +
            "</deployment>";

        final String secOnWithNoAdmin =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <security enabled=\"true\"/>" +
            "   <users>" +
            "      <user name=\"joe\" password=\"aaa\"/>" +
            "   </users>" +
            "</deployment>";

        final String secOn =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <security enabled=\"true\"/>" +
            "   <users>" +
            "      <user name=\"joe\" password=\"aaa\" roles=\"administrator\"/>" +
            "   </users>" +
            "</deployment>";

        final File tmpSecOff = VoltProjectBuilder.writeStringToTempFile(secOff);
        CatalogUtil.compileDeployment(catalog, tmpSecOff.getPath(), true, false);
        Cluster cluster =  catalog.getClusters().get("cluster");
        assertFalse(cluster.getSecurityenabled());

        setUp();
        final File tmpSecOnWithNoAdmin = VoltProjectBuilder.writeStringToTempFile(secOnWithNoAdmin);
        long result = CatalogUtil.compileDeployment(catalog, tmpSecOnWithNoAdmin.getPath(), true, false);
        assertEquals(-1, result);

        setUp();
        final File tmpSecOn = VoltProjectBuilder.writeStringToTempFile(secOn);
        CatalogUtil.compileDeployment(catalog, tmpSecOn.getPath(), true, false);
        cluster =  catalog.getClusters().get("cluster");
        assertTrue(cluster.getSecurityenabled());
    }

    public void testSecurityProvider() throws Exception
    {
        final String secOff =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <security enabled=\"true\"/>" +
            "   <users>" +
            "      <user name=\"joe\" password=\"aaa\" roles=\"administrator\"/>" +
            "   </users>" +
            "</deployment>";

        final String secOn =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <security enabled=\"true\" provider=\"kerberos\"/>" +
            "   <users>" +
            "      <user name=\"joe\" password=\"aaa\" roles=\"administrator\"/>" +
            "   </users>" +
            "</deployment>";

        final File tmpSecOff = VoltProjectBuilder.writeStringToTempFile(secOff);
        CatalogUtil.compileDeployment(catalog, tmpSecOff.getPath(), true, false);
        Cluster cluster =  catalog.getClusters().get("cluster");
        Database db = cluster.getDatabases().get("database");
        assertTrue(cluster.getSecurityenabled());
        assertEquals("hash", db.getSecurityprovider());

        setUp();
        final File tmpSecOn = VoltProjectBuilder.writeStringToTempFile(secOn);
        CatalogUtil.compileDeployment(catalog, tmpSecOn.getPath(), true, false);
        cluster =  catalog.getClusters().get("cluster");
        db = cluster.getDatabases().get("database");
        assertTrue(cluster.getSecurityenabled());
        assertEquals("kerberos", db.getSecurityprovider());
    }

    public void testUserRoles() throws Exception {
        final String depRole = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "<security enabled=\"true\"/>" +
            "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "<httpd port='0'>" +
            "<jsonapi enabled='true'/>" +
            "</httpd>" +
            "<users> " +
            "<user name=\"admin\" password=\"admin\" roles=\"administrator\"/>" +
            "<user name=\"joe\" password=\"aaa\" roles=\"lotre,lodue,louno,dontexist\"/>" +
            "<user name=\"jane\" password=\"bbb\" roles=\"launo,ladue,latre,dontexist\"/>" +
            "</users>" +
            "</deployment>";

        catalog_db.getGroups().add("louno");
        catalog_db.getGroups().add("lodue");
        catalog_db.getGroups().add("lotre");
        catalog_db.getGroups().add("launo");
        catalog_db.getGroups().add("ladue");
        catalog_db.getGroups().add("latre");

        final File tmpRole = VoltProjectBuilder.writeStringToTempFile(depRole);
        CatalogUtil.compileDeployment(catalog, tmpRole.getPath(), true, false);
        Database db = catalog.getClusters().get("cluster")
                .getDatabases().get("database");

        User joe = db.getUsers().get("joe");
        assertNotNull(joe);
        assertNotNull(joe.getGroups().get("louno"));
        assertNotNull(joe.getGroups().get("lodue"));
        assertNotNull(joe.getGroups().get("lotre"));
        assertNull(joe.getGroups().get("latre"));
        assertNull(joe.getGroups().get("dontexist"));

        User jane = db.getUsers().get("jane");
        assertNotNull(jane);
        assertNotNull(jane.getGroups().get("launo"));
        assertNotNull(jane.getGroups().get("ladue"));
        assertNotNull(jane.getGroups().get("latre"));
        assertNull(jane.getGroups().get("lotre"));
        assertNull(joe.getGroups().get("dontexist"));
    }

    public void testScrambledPasswords() throws Exception {
        final String depRole = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "<security enabled=\"true\"/>" +
            "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "<httpd port='0'>" +
            "<jsonapi enabled='true'/>" +
            "</httpd>" +
            "<users> " +
            "<user name=\"joe\" password=\"1E4E888AC66F8DD41E00C5A7AC36A32A9950D271\" plaintext=\"false\" roles=\"louno,administrator\"/>" +
            "<user name=\"jane\" password=\"AAF4C61DDCC5E8A2DABEDE0F3B482CD9AEA9434D\" plaintext=\"false\" roles=\"launo\"/>" +
            "</users>" +
            "</deployment>";

        catalog_db.getGroups().add("louno");
        catalog_db.getGroups().add("launo");

        final File tmpRole = VoltProjectBuilder.writeStringToTempFile(depRole);

        CatalogUtil.compileDeployment(catalog, tmpRole.getPath(), true, false);

        Database db = catalog.getClusters().get("cluster")
                .getDatabases().get("database");

        User joe = db.getUsers().get("joe");
        assertNotNull(joe);
        assertNotNull(joe.getGroups().get("louno"));
        assertNotNull(joe.getShadowpassword());

        User jane = db.getUsers().get("jane");
        assertNotNull(jane);
        assertNotNull(jane.getGroups().get("launo"));
        assertNotNull(joe.getShadowpassword());
    }

    public void testSystemSettingsMaxTempTableSize() throws Exception
    {
        final String depOff =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <snapshot frequency=\"5s\" retain=\"10\" prefix=\"pref2\" enabled=\"false\"/>" +
            "</deployment>";

        final String depOn =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <snapshot frequency=\"5s\" retain=\"10\" prefix=\"pref2\" enabled=\"true\"/>" +
            "   <systemsettings>" +
            "      <temptables maxsize=\"200\"/>" +
            "   </systemsettings>" +
            "</deployment>";

        final File tmpDepOff = VoltProjectBuilder.writeStringToTempFile(depOff);
        long crcDepOff = CatalogUtil.compileDeployment(catalog, tmpDepOff.getPath(), true, false);
        assertTrue(crcDepOff >= 0);
        Systemsettings sysset = catalog.getClusters().get("cluster").getDeployment().get("deployment").getSystemsettings().get("systemsettings");
        assertEquals(100, sysset.getTemptablemaxsize());

        setUp();
        final File tmpDepOn = VoltProjectBuilder.writeStringToTempFile(depOn);
        long crcDepOn = CatalogUtil.compileDeployment(catalog, tmpDepOn.getPath(), true, false);
        assertTrue(crcDepOn >= 0);
        sysset = catalog.getClusters().get("cluster").getDeployment().get("deployment").getSystemsettings().get("systemsettings");
        assertEquals(200, sysset.getTemptablemaxsize());
    }

    public void testSystemSettingsQueryTimeout() throws Exception
    {
        final String depOff =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <snapshot frequency=\"5s\" retain=\"10\" prefix=\"pref2\" enabled=\"false\"/>" +
            "</deployment>";

        final String depOn =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <snapshot frequency=\"5s\" retain=\"10\" prefix=\"pref2\" enabled=\"true\"/>" +
            "   <systemsettings>" +
            "      <query timeout=\"200\"/>" +
            "   </systemsettings>" +
            "</deployment>";

        final File tmpDepOff = VoltProjectBuilder.writeStringToTempFile(depOff);
        long crcDepOff = CatalogUtil.compileDeployment(catalog, tmpDepOff.getPath(), true, false);
        assertTrue(crcDepOff >= 0);
        Systemsettings sysset = catalog.getClusters().get("cluster").getDeployment().get("deployment").getSystemsettings().get("systemsettings");
        assertEquals(0, sysset.getQuerytimeout());

        setUp();
        final File tmpDepOn = VoltProjectBuilder.writeStringToTempFile(depOn);
        long crcDepOn = CatalogUtil.compileDeployment(catalog, tmpDepOn.getPath(), true, false);
        assertTrue(crcDepOn >= 0);
        sysset = catalog.getClusters().get("cluster").getDeployment().get("deployment").getSystemsettings().get("systemsettings");
        assertEquals(200, sysset.getQuerytimeout());
    }


    // XXX Need to add command log paths here when command logging
    // gets tweaked to create directories if they don't exist
    public void testRelativePathsToVoltDBRoot() throws Exception
    {
        final String voltdbroot = "/tmp/" + System.getProperty("user.name");
        final String snappath = "test_snapshots";
        final String exportpath = "test_export_overflow";
        final String commandlogpath = "test_command_log";
        final String commandlogsnapshotpath = "test_command_log_snapshot";

        File voltroot = new File(voltdbroot);
        for (File f : voltroot.listFiles())
        {
            f.delete();
        }

        final String deploy =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths>" +
            "       <voltdbroot path=\"" + voltdbroot + "\" />" +
            "       <snapshots path=\"" + snappath + "\"/>" +
            "       <exportoverflow path=\"" + exportpath + "\"/>" +
            "       <commandlog path=\"" + commandlogpath + "\"/>" +
            "       <commandlogsnapshot path=\"" + commandlogsnapshotpath + "\"/>" +
            "   </paths>" +
            "</deployment>";

        final File tmpDeploy = VoltProjectBuilder.writeStringToTempFile(deploy);
        CatalogUtil.compileDeployment(catalog, tmpDeploy.getPath(), true, false);

        File snapdir = new File(voltdbroot, snappath);
        assertTrue("snapshot directory: " + snapdir.getAbsolutePath() + " does not exist",
                   snapdir.exists());
        assertTrue("snapshot directory: " + snapdir.getAbsolutePath() + " is not a directory",
                   snapdir.isDirectory());
        File exportdir = new File(voltdbroot, exportpath);
        assertTrue("export overflow directory: " + exportdir.getAbsolutePath() + " does not exist",
                   exportdir.exists());
        assertTrue("export overflow directory: " + exportdir.getAbsolutePath() + " is not a directory",
                   exportdir.isDirectory());
        if (VoltDB.instance().getConfig().m_isEnterprise)
        {
            File commandlogdir = new File(voltdbroot, commandlogpath);
            assertTrue("command log directory: " + commandlogdir.getAbsolutePath() + " does not exist",
                       commandlogdir.exists());
            assertTrue("command log directory: " + commandlogdir.getAbsolutePath() + " is not a directory",
                       commandlogdir.isDirectory());
            File commandlogsnapshotdir = new File(voltdbroot, commandlogsnapshotpath);
            assertTrue("command log snapshot directory: " +
                       commandlogsnapshotdir.getAbsolutePath() + " does not exist",
                       commandlogsnapshotdir.exists());
            assertTrue("command log snapshot directory: " +
                       commandlogsnapshotdir.getAbsolutePath() + " is not a directory",
                       commandlogsnapshotdir.isDirectory());
        }
    }

    public void testCompileDeploymentAgainstEmptyCatalog() {
        Catalog catalog = new Catalog();
        Cluster cluster = catalog.getClusters().add("cluster");
        cluster.getDatabases().add("database");

        String deploymentContent =
            "<?xml version=\"1.0\"?>\n" +
            "<deployment>\n" +
            "    <cluster hostcount='1' sitesperhost='1' kfactor='0' />\n" +
            "    <httpd enabled='true'>\n" +
            "        <jsonapi enabled='true' />\n" +
            "    </httpd>\n" +
            "    <export enabled='false'/>\n" +
            "</deployment>\n";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(deploymentContent);
        final String depPath = schemaFile.getPath();

        CatalogUtil.compileDeployment(catalog, depPath, false, false);

        String commands = catalog.serialize();
        System.out.println(commands);

    }

    public void testCatalogVersionCheck() {
        // non-sensical version shouldn't work
        assertFalse(CatalogUtil.isCatalogVersionValid("nonsense"));

        // current version should work
        assertTrue(CatalogUtil.isCatalogVersionValid(VoltDB.instance().getVersionString()));
    }

    // I'm not testing the legacy behavior here, just IV2
    public void testIv2PartitionDetectionSettings() throws Exception
    {
        final String noElement =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "</deployment>";

        final String ppdEnabledDefaultPrefix =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <partition-detection enabled='true'>" +
            "   </partition-detection>" +
            "</deployment>";

        final String ppdEnabledWithPrefix =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <partition-detection enabled='true'>" +
            "      <snapshot prefix='testPrefix'/>" +
            "   </partition-detection>" +
            "</deployment>";

        final String ppdDisabledNoPrefix =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <partition-detection enabled='false'>" +
            "   </partition-detection>" +
            "</deployment>";

        final File tmpNoElement = VoltProjectBuilder.writeStringToTempFile(noElement);
        long crc = CatalogUtil.compileDeployment(catalog, tmpNoElement.getPath(), true, false);
        assertTrue("Deployment file failed to parse", crc != -1);
        Cluster cluster = catalog.getClusters().get("cluster");
        assertTrue(cluster.getNetworkpartition());
        assertEquals("partition_detection", cluster.getFaultsnapshots().get("CLUSTER_PARTITION").getPrefix());

        setUp();
        final File tmpEnabledDefault = VoltProjectBuilder.writeStringToTempFile(ppdEnabledDefaultPrefix);
        crc = CatalogUtil.compileDeployment(catalog, tmpEnabledDefault.getPath(), true, false);
        assertTrue("Deployment file failed to parse", crc != -1);
        cluster = catalog.getClusters().get("cluster");
        assertTrue(cluster.getNetworkpartition());
        assertEquals("partition_detection", cluster.getFaultsnapshots().get("CLUSTER_PARTITION").getPrefix());

        setUp();
        final File tmpEnabledPrefix = VoltProjectBuilder.writeStringToTempFile(ppdEnabledWithPrefix);
        crc = CatalogUtil.compileDeployment(catalog, tmpEnabledPrefix.getPath(), true, false);
        assertTrue("Deployment file failed to parse", crc != -1);
        cluster = catalog.getClusters().get("cluster");
        assertTrue(cluster.getNetworkpartition());
        assertEquals("testPrefix", cluster.getFaultsnapshots().get("CLUSTER_PARTITION").getPrefix());

        setUp();
        final File tmpDisabled = VoltProjectBuilder.writeStringToTempFile(ppdDisabledNoPrefix);
        crc = CatalogUtil.compileDeployment(catalog, tmpDisabled.getPath(), true, false);
        assertTrue("Deployment file failed to parse", crc != -1);
        cluster = catalog.getClusters().get("cluster");
        assertFalse(cluster.getNetworkpartition());
    }

    public void testCustomExportClientSettings() throws Exception {
        if (!MiscUtils.isPro()) { return; } // not supported in community

        final String withBadCustomExport =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export enabled='true' target='custom' exportconnectorclass=\"com.foo.export.ExportClient\" >"
                + "        <configuration>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String withGoodCustomExport =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export enabled='true' target='custom' exportconnectorclass=\"org.voltdb.exportclient.NoOpTestExportClient\" >"
                + "        <configuration>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String withBuiltinFileExport =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export enabled='true' target='file'>"
                + "        <configuration>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String withBuiltinKafkaExport =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export enabled='true' target='kafka'>"
                + "        <configuration>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String withBuiltinRabbitMQExport =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export enabled='true' target='rabbitmq'>"
                + "        <configuration>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String ddl =
                "CREATE TABLE export_data ( id BIGINT default 0 , value BIGINT DEFAULT 0 );\n"
                + "EXPORT TABLE export_data;";

        final File tmpDdl = VoltProjectBuilder.writeStringToTempFile(ddl);

        //Custom deployment with bad class export will be disabled.
        final File tmpBad = VoltProjectBuilder.writeStringToTempFile(withBadCustomExport);
        DeploymentType bad_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpBad));

        VoltCompiler compiler = new VoltCompiler();
        String x[] = {tmpDdl.getAbsolutePath()};
        Catalog cat = compiler.compileCatalogFromDDL(x);

        long crc = CatalogUtil.compileDeployment(cat, bad_deployment, true, false);
        assertTrue("Deployment file failed to parse", crc != -1);

        Database db = cat.getClusters().get("cluster").getDatabases().get("database");
        org.voltdb.catalog.Connector catconn = db.getConnectors().get("0");
        assertNotNull(catconn);

        assertFalse(bad_deployment.getExport().isEnabled());

        //This is a good deployment with custom class that can be found
        final File tmpGood = VoltProjectBuilder.writeStringToTempFile(withGoodCustomExport);
        DeploymentType good_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpGood));

        Catalog cat2 = compiler.compileCatalogFromDDL(x);
        crc = CatalogUtil.compileDeployment(cat2, good_deployment, true, false);
        assertTrue("Deployment file failed to parse", crc != -1);

        db = cat2.getClusters().get("cluster").getDatabases().get("database");
        catconn = db.getConnectors().get("0");
        assertNotNull(catconn);

        assertTrue(good_deployment.getExport().isEnabled());
        assertEquals(good_deployment.getExport().getTarget(), ServerExportEnum.CUSTOM);
        assertEquals(good_deployment.getExport().getExportconnectorclass(),
                "org.voltdb.exportclient.NoOpTestExportClient");
        ConnectorProperty prop = catconn.getConfig().get(ExportDataProcessor.EXPORT_TO_TYPE);
        assertEquals(prop.getValue(), "org.voltdb.exportclient.NoOpTestExportClient");

        // This is to test previous deployment with builtin export functionality.
        final File tmpBuiltin = VoltProjectBuilder.writeStringToTempFile(withBuiltinFileExport);
        DeploymentType builtin_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpBuiltin));

        Catalog cat3 = compiler.compileCatalogFromDDL(x);
        crc = CatalogUtil.compileDeployment(cat3, builtin_deployment, true, false);
        assertTrue("Deployment file failed to parse", crc != -1);

        db = cat3.getClusters().get("cluster").getDatabases().get("database");
        catconn = db.getConnectors().get("0");
        assertNotNull(catconn);

        assertTrue(builtin_deployment.getExport().isEnabled());
        assertEquals(builtin_deployment.getExport().getTarget(), ServerExportEnum.FILE);
        prop = catconn.getConfig().get(ExportDataProcessor.EXPORT_TO_TYPE);
        assertEquals(prop.getValue(), "org.voltdb.exportclient.ExportToFileClient");

        //Check kafka option.
        final File tmpKafkaBuiltin = VoltProjectBuilder.writeStringToTempFile(withBuiltinKafkaExport);
        DeploymentType builtin_kafkadeployment = CatalogUtil.getDeployment(new FileInputStream(tmpKafkaBuiltin));

        Catalog cat4 = compiler.compileCatalogFromDDL(x);
        crc = CatalogUtil.compileDeployment(cat4, builtin_kafkadeployment, true, false);
        assertTrue("Deployment file failed to parse", crc != -1);

        db = cat4.getClusters().get("cluster").getDatabases().get("database");
        catconn = db.getConnectors().get("0");
        assertNotNull(catconn);

        assertTrue(builtin_kafkadeployment.getExport().isEnabled());
        assertEquals(builtin_kafkadeployment.getExport().getTarget(), ServerExportEnum.KAFKA);
        prop = catconn.getConfig().get(ExportDataProcessor.EXPORT_TO_TYPE);
        assertEquals(prop.getValue(), "org.voltdb.exportclient.KafkaExportClient");

        // Check RabbitMQ option
        final File tmpRabbitMQBuiltin = VoltProjectBuilder.writeStringToTempFile(withBuiltinRabbitMQExport);
        DeploymentType builtin_rabbitmqdeployment = CatalogUtil.getDeployment(new FileInputStream(tmpRabbitMQBuiltin));
        Catalog cat5 = compiler.compileCatalogFromDDL(x);
        crc = CatalogUtil.compileDeployment(cat5, builtin_rabbitmqdeployment, true, false);
        assertTrue("Deployment file failed to parse", crc != -1);
        db = cat5.getClusters().get("cluster").getDatabases().get("database");
        catconn = db.getConnectors().get("0");
        assertNotNull(catconn);
        assertTrue(builtin_rabbitmqdeployment.getExport().isEnabled());
        assertEquals(ServerExportEnum.RABBITMQ, builtin_rabbitmqdeployment.getExport().getTarget());
        prop = catconn.getConfig().get(ExportDataProcessor.EXPORT_TO_TYPE);
        assertEquals("org.voltdb.exportclient.RabbitMQExportClient", prop.getValue());
    }

    /**
     * The CRC of an empty catalog should always be the same.
     */
    public void testEmptyCatalogCRC() throws Exception {
        File file1 = CatalogUtil.createTemporaryEmptyCatalogJarFile();
        assertNotNull(file1);
        byte[] bytes1 = MiscUtils.fileToBytes(file1);
        InMemoryJarfile jar1 = new InMemoryJarfile(bytes1);
        long crc1 = jar1.getCRC();
        Thread.sleep(5000);
        File file2 = CatalogUtil.createTemporaryEmptyCatalogJarFile();
        assertNotNull(file2);
        byte[] bytes2 = MiscUtils.fileToBytes(file2);
        InMemoryJarfile jar2 = new InMemoryJarfile(bytes2);
        long crc2 = jar2.getCRC();
        assertEquals(crc1, crc2);
    }

    public void testClusterSchemaSetting() throws Exception
    {
        final String defSchema =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "</deployment>";

        final String catalogSchema =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2' schema='catalog'/>" +
            "</deployment>";

        final String adhocSchema =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2' schema='ddl'/>" +
            "</deployment>";

        final File tmpDefSchema = VoltProjectBuilder.writeStringToTempFile(defSchema);
        CatalogUtil.compileDeployment(catalog, tmpDefSchema.getPath(), true, false);
        Cluster cluster =  catalog.getClusters().get("cluster");
        assertTrue(cluster.getUseddlschema());

        setUp();
        final File tmpCatalogSchema = VoltProjectBuilder.writeStringToTempFile(catalogSchema);
        CatalogUtil.compileDeployment(catalog, tmpCatalogSchema.getPath(), true, false);
        cluster =  catalog.getClusters().get("cluster");
        assertFalse(cluster.getUseddlschema());

        setUp();
        final File tmpAdhocSchema = VoltProjectBuilder.writeStringToTempFile(adhocSchema);
        CatalogUtil.compileDeployment(catalog, tmpAdhocSchema.getPath(), true, false);
        cluster =  catalog.getClusters().get("cluster");
        assertTrue(cluster.getUseddlschema());
    }

    public void testProcedureReadWriteAccess() {

        assertFalse(checkTableInProcedure("InsertStock", "STOCK", true));
        assertFalse(checkTableInProcedure("InsertStock", "NEW_ORDER", false));

        assertTrue(checkTableInProcedure("SelectAll", "HISTORY", true));
        assertTrue(checkTableInProcedure("SelectAll", "NEW_ORDER", true));
        assertFalse(checkTableInProcedure("SelectAll", "HISTORY", false));

        assertTrue(checkTableInProcedure("neworder", "WAREHOUSE", true));
        assertFalse(checkTableInProcedure("neworder", "ORDERS", true));
        assertFalse(checkTableInProcedure("neworder", "WAREHOUSE", false));

        assertFalse(checkTableInProcedure("paymentByCustomerIdW", "WAREHOUSE", true));
        assertFalse(checkTableInProcedure("paymentByCustomerIdW", "HISTORY", true));
        assertTrue(checkTableInProcedure("paymentByCustomerIdW", "WAREHOUSE", false));
        assertTrue(checkTableInProcedure("paymentByCustomerIdW", "HISTORY", false));

        assertFalse(checkTableInProcedure("ResetWarehouse", "ORDER_LINE", true));
        assertTrue(checkTableInProcedure("ResetWarehouse", "ORDER_LINE", false));
    }

    private boolean checkTableInProcedure(String procedureName, String tableName, boolean read){

        ProcedureAnnotation annotation = (ProcedureAnnotation) catalog_db
                .getProcedures().get(procedureName).getAnnotation();

        SortedSet<Table> tables = null;
        if(read){
            tables = annotation.tablesRead;
        } else {
            tables = annotation.tablesUpdated;
        }

        boolean containsTable = false;
        for(Table t: tables) {
            if(t.getTypeName().equals(tableName)) {
                containsTable = true;
                break;
            }
        }
        return containsTable;
    }

    public void testDRConnection() throws Exception {
        final String noConnections =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <dr name='test' id='0'>"
                + "    </dr>"
                + "</deployment>";
        final String multipleConnections =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <dr name='test' id='0'>"
                + "        <connection source='master'/>"
                + "        <connection source='imposter'/>"
                + "    </dr>"
                + "</deployment>";
        final String oneConnection =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <dr name='test' id='0'>"
                + "        <connection source='master'/>"
                + "    </dr>"
                + "</deployment>";
        final String oneEnabledConnection =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <dr>"
                + "        <connection source='master' enabled='true'/>"
                + "    </dr>"
                + "</deployment>";
        final String oneDisabledConnection =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <dr>"
                + "        <connection source='master' enabled='false'/>"
                + "    </dr>"
                + "</deployment>";

        final File tmpNoParse = VoltProjectBuilder.writeStringToTempFile(noConnections);
        assertNull(CatalogUtil.getDeployment(new FileInputStream(tmpNoParse)));

        final File tmpInvalidMultiple = VoltProjectBuilder.writeStringToTempFile(multipleConnections);
        assertNull(CatalogUtil.getDeployment(new FileInputStream(tmpInvalidMultiple)));

        assertTrue(catalog.getClusters().get("cluster").getDrmasterhost().isEmpty());

        final File tmpDefault = VoltProjectBuilder.writeStringToTempFile(oneConnection);
        DeploymentType valid_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpDefault));
        assertNotNull(valid_deployment);

        long crc = CatalogUtil.compileDeployment(catalog, valid_deployment, true, false);
        assertTrue("Deployment file failed to parse", crc != -1);

        assertEquals("master", catalog.getClusters().get("cluster").getDrmasterhost());

        final File tmpEnabled = VoltProjectBuilder.writeStringToTempFile(oneEnabledConnection);
        DeploymentType valid_deployment_enabled = CatalogUtil.getDeployment(new FileInputStream(tmpEnabled));
        assertNotNull(valid_deployment_enabled);

        setUp();
        crc = CatalogUtil.compileDeployment(catalog, valid_deployment_enabled, true, false);
        assertTrue("Deployment file failed to parse", crc != -1);

        assertEquals("master", catalog.getClusters().get("cluster").getDrmasterhost());

        final File tmpDisabled = VoltProjectBuilder.writeStringToTempFile(oneDisabledConnection);
        DeploymentType valid_deployment_disabled = CatalogUtil.getDeployment(new FileInputStream(tmpDisabled));
        assertNotNull(valid_deployment_disabled);

        setUp();
        crc = CatalogUtil.compileDeployment(catalog, valid_deployment_disabled, true, false);
        assertTrue("Deployment file failed to parse", crc != -1);

        assertTrue(catalog.getClusters().get("cluster").getDrmasterhost().isEmpty());
    }

    public void testDRTableSignatureCrc() throws IOException
    {
        // No DR tables, CRC should be 0
        assertEquals(Pair.of(0l, ""), CatalogUtil.calculateDrTableSignatureAndCrc(catalog_db));

        // Replicated tables cannot be DRed for now, so they are always skipped in the catalog compilation.
        // Add replicated tables to the test once we start supporting them.

        // Different order should match
        verifyDrTableSignature(true,
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                               "DR TABLE *;\n",
                               "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "DR TABLE *;\n");

        // Missing one table
        verifyDrTableSignature(false,
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                               "DR TABLE *;\n",
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "DR TABLE *;\n");

        // Different column type
        verifyDrTableSignature(false,
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 FLOAT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                               "DR TABLE *;\n",
                               "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "DR TABLE *;\n");
    }

    private void verifyDrTableSignature(boolean shouldEqual, String schemaA, String schemaB) throws IOException
    {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        final File fileA = VoltFile.createTempFile("catA", ".jar", new File(testDir));
        final File fileB = VoltFile.createTempFile("catB", ".jar", new File(testDir));

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(schemaA);
        builder.compile(fileA.getPath());
        Catalog catA = TestCatalogDiffs.catalogForJar(fileA.getPath());

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(schemaB);
        builder.compile(fileB.getPath());
        Catalog catB = TestCatalogDiffs.catalogForJar(fileB.getPath());

        fileA.delete();
        fileB.delete();

        final Pair<Long, String> sigA = CatalogUtil.calculateDrTableSignatureAndCrc(catA.getClusters().get("cluster").getDatabases().get("database"));
        final Pair<Long, String> sigB = CatalogUtil.calculateDrTableSignatureAndCrc(catB.getClusters().get("cluster").getDatabases().get("database"));

        assertFalse(sigA.getFirst() == 0);
        assertFalse(sigA.getSecond().isEmpty());
        assertEquals(shouldEqual, sigA.equals(sigB));
    }

    public void testDRTableSignatureDeserialization() throws IOException
    {
        verifyDeserializedDRTableSignature("CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                                           "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                                           "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n");

        verifyDeserializedDRTableSignature("CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                                           "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                                           "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                                           "DR TABLE B;\n",
                                           Pair.of("B", "bs"));

        verifyDeserializedDRTableSignature("CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                                           "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                                           "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                                           "DR TABLE *;\n",
                                           Pair.of("A", "ip"),
                                           Pair.of("B", "bs"),
                                           Pair.of("C", "tv"));
    }

    @SafeVarargs
    private final void verifyDeserializedDRTableSignature(String schema, Pair<String, String>... signatures) throws IOException
    {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        final File file = VoltFile.createTempFile("deserializeCat", ".jar", new File(testDir));

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(schema);
        builder.compile(file.getPath());
        Catalog cat = TestCatalogDiffs.catalogForJar(file.getPath());

        file.delete();

        final Map<String, String> sig = CatalogUtil.deserializeCatalogSignature(CatalogUtil.calculateDrTableSignatureAndCrc(
            cat.getClusters().get("cluster").getDatabases().get("database")).getSecond());

        assertEquals(signatures.length, sig.size());
        for (Pair<String, String> expected : signatures) {
            assertEquals(expected.getSecond(), sig.get(expected.getFirst()));
        }
    }
}
