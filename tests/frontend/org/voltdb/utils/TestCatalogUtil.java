/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.List;
import java.util.SortedSet;

import junit.framework.TestCase;

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
import org.voltdb.catalog.User;
import org.voltdb.common.Constants;
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
            CatalogSchemaTools.toSchema(sb, catalog_tbl, null, null);
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

        // Make sure the default is 90 seconds
        final String def =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <admin-mode port='32323' adminstartup='true'/>" +
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
        final File tmpDef = VoltProjectBuilder.writeStringToTempFile(def);
        final File tmpBoom = VoltProjectBuilder.writeStringToTempFile(boom);

        String msg = CatalogUtil.compileDeployment(catalog, tmpDep.getPath(), false);

        assertEquals(30, catalog.getClusters().get("cluster").getHeartbeattimeout());

        catalog = new Catalog();
        Cluster cluster = catalog.getClusters().add("cluster");
        cluster.getDatabases().add("database");
        msg = CatalogUtil.compileDeployment(catalog, tmpDef.getPath(), false);
        assertEquals(org.voltcore.common.Constants.DEFAULT_HEARTBEAT_TIMEOUT_SECONDS,
                catalog.getClusters().get("cluster").getHeartbeattimeout());

        // This returns -1 on schema violation
        msg = CatalogUtil.compileDeployment(catalog, tmpBoom.getPath(), false);
        assertTrue(msg != null);
        assertTrue(msg.contains("Error parsing deployment file"));
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
        CatalogUtil.compileDeployment(catalog, tmpDepOff.getPath(), false);
        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");
        assertFalse(db.getSnapshotschedule().get("default").getEnabled());

        setUp();
        final File tmpDepOn = VoltProjectBuilder.writeStringToTempFile(depOn);
        CatalogUtil.compileDeployment(catalog, tmpDepOn.getPath(), false);
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
        CatalogUtil.compileDeployment(catalog, tmpSecOff.getPath(), false);
        Cluster cluster =  catalog.getClusters().get("cluster");
        assertFalse(cluster.getSecurityenabled());

        setUp();
        final File tmpSecOnWithNoAdmin = VoltProjectBuilder.writeStringToTempFile(secOnWithNoAdmin);
        String result = CatalogUtil.compileDeployment(catalog, tmpSecOnWithNoAdmin.getPath(), false);
        assertTrue(result != null);
        assertTrue(result.contains("Cannot enable security without defining"));

        setUp();
        final File tmpSecOn = VoltProjectBuilder.writeStringToTempFile(secOn);
        CatalogUtil.compileDeployment(catalog, tmpSecOn.getPath(), false);
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
        CatalogUtil.compileDeployment(catalog, tmpSecOff.getPath(), false);
        Cluster cluster =  catalog.getClusters().get("cluster");
        Database db = cluster.getDatabases().get("database");
        assertTrue(cluster.getSecurityenabled());
        assertEquals("hash", db.getSecurityprovider());

        setUp();
        final File tmpSecOn = VoltProjectBuilder.writeStringToTempFile(secOn);
        CatalogUtil.compileDeployment(catalog, tmpSecOn.getPath(), false);
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
        CatalogUtil.compileDeployment(catalog, tmpRole.getPath(), false);
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

        CatalogUtil.compileDeployment(catalog, tmpRole.getPath(), false);

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
        String msg = CatalogUtil.compileDeployment(catalog, tmpDepOff.getPath(), false);
        assertTrue(msg == null);
        Systemsettings sysset = catalog.getClusters().get("cluster").getDeployment().get("deployment").getSystemsettings().get("systemsettings");
        assertEquals(100, sysset.getTemptablemaxsize());

        setUp();
        final File tmpDepOn = VoltProjectBuilder.writeStringToTempFile(depOn);
        msg = CatalogUtil.compileDeployment(catalog, tmpDepOn.getPath(), false);
        assertTrue(msg == null);
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
        String msg = CatalogUtil.compileDeployment(catalog, tmpDepOff.getPath(), false);
        assertTrue(msg == null);
        Systemsettings sysset = catalog.getClusters().get("cluster").getDeployment().get("deployment").getSystemsettings().get("systemsettings");
        assertEquals(0, sysset.getQuerytimeout());

        setUp();
        final File tmpDepOn = VoltProjectBuilder.writeStringToTempFile(depOn);
        msg = CatalogUtil.compileDeployment(catalog, tmpDepOn.getPath(), false);
        assertTrue(msg == null);
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
        CatalogUtil.compileDeployment(catalog, tmpDeploy.getPath(), false);

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

        CatalogUtil.compileDeployment(catalog, depPath, false);

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
        String msg = CatalogUtil.compileDeployment(catalog, tmpNoElement.getPath(), false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);
        Cluster cluster = catalog.getClusters().get("cluster");
        assertTrue(cluster.getNetworkpartition());
        assertEquals("partition_detection", cluster.getFaultsnapshots().get("CLUSTER_PARTITION").getPrefix());

        setUp();
        final File tmpEnabledDefault = VoltProjectBuilder.writeStringToTempFile(ppdEnabledDefaultPrefix);
        msg = CatalogUtil.compileDeployment(catalog, tmpEnabledDefault.getPath(), false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);
        cluster = catalog.getClusters().get("cluster");
        assertTrue(cluster.getNetworkpartition());
        assertEquals("partition_detection", cluster.getFaultsnapshots().get("CLUSTER_PARTITION").getPrefix());

        setUp();
        final File tmpEnabledPrefix = VoltProjectBuilder.writeStringToTempFile(ppdEnabledWithPrefix);
        msg = CatalogUtil.compileDeployment(catalog, tmpEnabledPrefix.getPath(), false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);
        cluster = catalog.getClusters().get("cluster");
        assertTrue(cluster.getNetworkpartition());
        assertEquals("testPrefix", cluster.getFaultsnapshots().get("CLUSTER_PARTITION").getPrefix());

        setUp();
        final File tmpDisabled = VoltProjectBuilder.writeStringToTempFile(ppdDisabledNoPrefix);
        msg = CatalogUtil.compileDeployment(catalog, tmpDisabled.getPath(), false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);
        cluster = catalog.getClusters().get("cluster");
        assertFalse(cluster.getNetworkpartition());
    }

    public void testCustomExportClientSettings() throws Exception {
        if (!MiscUtils.isPro()) { return; } // not supported in community

        final String withBadCustomExport =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration stream='default' enabled='true' type='custom' exportconnectorclass=\"com.foo.export.ExportClient\" >"
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
                + "    <export>"
                + "        <configuration stream='default' enabled='true' type='custom' exportconnectorclass=\"org.voltdb.exportclient.NoOpTestExportClient\" >"
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
                + "    <export>"
                + "        <configuration stream='default' enabled='true' type='file'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">pre-fix</property>"
                + "            <property name=\"outdir\">exportdata</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String withBuiltinKafkaExport =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export enabled='true' target='kafka'>"
                + "        <configuration>"
                + "            <property name=\"metadata.broker.list\">uno,due,tre</property>"
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

        String msg = CatalogUtil.compileDeployment(cat, bad_deployment, false);
        assertTrue("compilation should have failed", msg.contains("Custom Export failed to configure"));

        //This is a good deployment with custom class that can be found
        final File tmpGood = VoltProjectBuilder.writeStringToTempFile(withGoodCustomExport);
        DeploymentType good_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpGood));

        Catalog cat2 = compiler.compileCatalogFromDDL(x);
        msg = CatalogUtil.compileDeployment(cat2, good_deployment, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        Database db = cat2.getClusters().get("cluster").getDatabases().get("database");
        org.voltdb.catalog.Connector catconn = db.getConnectors().get(Constants.DEFAULT_EXPORT_CONNECTOR_NAME);
        assertNotNull(catconn);

        assertTrue(good_deployment.getExport().getConfiguration().get(0).isEnabled());
        assertEquals(good_deployment.getExport().getConfiguration().get(0).getType(), ServerExportEnum.CUSTOM);
        assertEquals(good_deployment.getExport().getConfiguration().get(0).getExportconnectorclass(),
                "org.voltdb.exportclient.NoOpTestExportClient");
        ConnectorProperty prop = catconn.getConfig().get(ExportDataProcessor.EXPORT_TO_TYPE);
        assertEquals(prop.getValue(), "org.voltdb.exportclient.NoOpTestExportClient");

        // This is to test previous deployment with builtin export functionality.
        final File tmpBuiltin = VoltProjectBuilder.writeStringToTempFile(withBuiltinFileExport);
        DeploymentType builtin_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpBuiltin));

        Catalog cat3 = compiler.compileCatalogFromDDL(x);
        msg = CatalogUtil.compileDeployment(cat3, builtin_deployment, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        db = cat3.getClusters().get("cluster").getDatabases().get("database");
        catconn = db.getConnectors().get(Constants.DEFAULT_EXPORT_CONNECTOR_NAME);
        assertNotNull(catconn);

        assertTrue(builtin_deployment.getExport().getConfiguration().get(0).isEnabled());
        assertEquals(builtin_deployment.getExport().getConfiguration().get(0).getType(), ServerExportEnum.FILE);
        prop = catconn.getConfig().get(ExportDataProcessor.EXPORT_TO_TYPE);
        assertEquals(prop.getValue(), "org.voltdb.exportclient.ExportToFileClient");

        //Check kafka option.
        final File tmpKafkaBuiltin = VoltProjectBuilder.writeStringToTempFile(withBuiltinKafkaExport);
        DeploymentType builtin_kafkadeployment = CatalogUtil.getDeployment(new FileInputStream(tmpKafkaBuiltin));

        Catalog cat4 = compiler.compileCatalogFromDDL(x);
        msg = CatalogUtil.compileDeployment(cat4, builtin_kafkadeployment, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        db = cat4.getClusters().get("cluster").getDatabases().get("database");
        catconn = db.getConnectors().get(Constants.DEFAULT_EXPORT_CONNECTOR_NAME);
        assertNotNull(catconn);

        assertTrue(builtin_kafkadeployment.getExport().getConfiguration().get(0).isEnabled());
        assertEquals(builtin_kafkadeployment.getExport().getConfiguration().get(0).getType(), ServerExportEnum.KAFKA);
        prop = catconn.getConfig().get(ExportDataProcessor.EXPORT_TO_TYPE);
        assertEquals(prop.getValue(), "org.voltdb.exportclient.KafkaExportClient");
    }

    public void testMultiExportClientSettings() throws Exception {
        if (!MiscUtils.isPro()) { return; } // not supported in community

        final String withBadCustomExport =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration stream='foo' enabled='true' type='custom' exportconnectorclass=\"com.foo.export.ExportClient\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "        <configuration stream='bar' enabled='true' type='file'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String withBadRepeatGroup =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration stream='foo' enabled='true' type='file'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">nonce</property>"
                + "            <property name=\"outdir\">/tmp</property>"
                + "        </configuration>"
                + "        <configuration stream='foo' enabled='true' type='kafka'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">nonce</property>"
                + "            <property name=\"outdir\">/tmp</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String withBadRepeatGroupDefault =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration stream='default' enabled='true' type='file'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">nonce</property>"
                + "            <property name=\"outdir\">/tmp</property>"
                + "        </configuration>"
                + "        <configuration stream='default' enabled='true' type='kafka'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">nonce</property>"
                + "            <property name=\"outdir\">/tmp</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String withBadMixedSyntax =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export enabled='true' stream='file'>"
                + "        <configuration stream='foo' enabled='true' type='custom' exportconnectorclass=\"org.voltdb.exportclient.NoOpTestExportClient\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "        <configuration stream='bar' enabled='true' type='file'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String withUnusedConnector =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration stream='foo' enabled='true' type='file'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">nonce</property>"
                + "            <property name=\"outdir\">/tmp</property>"
                + "        </configuration>"
                + "        <configuration stream='bar' enabled='true' type='file'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">nonce</property>"
                + "            <property name=\"outdir\">/tmp</property>"
                + "        </configuration>"
                + "        <configuration stream='unused' enabled='true' type='file'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">nonce</property>"
                + "            <property name=\"outdir\">/tmp</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String withGoodExport =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration stream='foo' enabled='true' type='custom' exportconnectorclass=\"org.voltdb.exportclient.NoOpTestExportClient\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "        <configuration stream='bar' enabled='true' type='file'>"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">nonce</property>"
                + "            <property name=\"outdir\">/tmp</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";
        final String ddl =
                "CREATE TABLE export_data ( id BIGINT default 0 , value BIGINT DEFAULT 0 );\n"
                + "EXPORT TABLE export_data TO STREAM foo;\n"
                + "CREATE TABLE export_more_data ( id BIGINT default 0 , value BIGINT DEFAULT 0 );\n"
                + "EXPORT TABLE export_more_data TO STREAM bar;";

        final File tmpDdl = VoltProjectBuilder.writeStringToTempFile(ddl);

        //Custom deployment with bad class export will be disabled.
        final File tmpBad = VoltProjectBuilder.writeStringToTempFile(withBadCustomExport);
        DeploymentType bad_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpBad));

        VoltCompiler compiler = new VoltCompiler();
        String x[] = {tmpDdl.getAbsolutePath()};
        Catalog cat = compiler.compileCatalogFromDDL(x);

        String msg = CatalogUtil.compileDeployment(cat, bad_deployment, false);
        if (msg == null) {
            fail("Should not accept a deployment file containing a missing export connector class.");
        } else {
            assertTrue(msg.contains("Custom Export failed to configure, failed to load export plugin class:"));
        }

        //This is a bad deployment with the same export stream defined multiple times
        final File tmpBadGrp = VoltProjectBuilder.writeStringToTempFile(withBadRepeatGroup);
        DeploymentType bad_grp_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpBadGrp));

        Catalog cat2 = compiler.compileCatalogFromDDL(x);
        if ((msg = CatalogUtil.compileDeployment(cat2, bad_grp_deployment, false)) == null) {
            fail("Should not accept a deployment file containing multiple connectors for the same stream.");
        } else {
            assertTrue(msg.contains("Multiple connectors can not be assigned to single export stream:"));
        }

        //This is a bad deployment with the same default export stream defined multiple times
        final File tmpBadGrpDef = VoltProjectBuilder.writeStringToTempFile(withBadRepeatGroupDefault);
        DeploymentType bad_grp_deployment_def = CatalogUtil.getDeployment(new FileInputStream(tmpBadGrpDef));

        Catalog cat2Def = compiler.compileCatalogFromDDL(x);
        if ((msg = CatalogUtil.compileDeployment(cat2Def, bad_grp_deployment_def, false)) == null) {
            fail("Should not accept a deployment file containing multiple connectors for the same stream.");
        } else {
            assertTrue(msg.contains("Multiple connectors can not be assigned to single export stream:"));
        }

        //This is a bad deployment that uses both the old and new syntax
        final File tmpBadSyntax = VoltProjectBuilder.writeStringToTempFile(withBadMixedSyntax);
        try{
            DeploymentType bad_syntax_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpBadSyntax));
            assertNull(bad_syntax_deployment);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Invalid schema, cannot use deprecated export syntax with multiple configuration tags."));
        }

        // This is to test that unused connectors are ignored
        final File tmpUnused = VoltProjectBuilder.writeStringToTempFile(withUnusedConnector);
        DeploymentType unused_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpUnused));

        Catalog cat3 = compiler.compileCatalogFromDDL(x);
        msg = CatalogUtil.compileDeployment(cat3, unused_deployment, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        Database db = cat3.getClusters().get("cluster").getDatabases().get("database");
        org.voltdb.catalog.Connector catconn = db.getConnectors().get("unused");
        assertNull(catconn);

        //This is a good deployment with custom class that can be found
        final File tmpGood = VoltProjectBuilder.writeStringToTempFile(withGoodExport);
        DeploymentType good_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpGood));

        Catalog cat4 = compiler.compileCatalogFromDDL(x);
        msg = CatalogUtil.compileDeployment(cat4, good_deployment, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        db = cat4.getClusters().get("cluster").getDatabases().get("database");

        catconn = db.getConnectors().get("foo");
        assertNotNull(catconn);
        assertTrue(good_deployment.getExport().getConfiguration().get(0).isEnabled());
        ConnectorProperty prop = catconn.getConfig().get(ExportDataProcessor.EXPORT_TO_TYPE);
        assertEquals(prop.getValue(), "org.voltdb.exportclient.NoOpTestExportClient");
        assertEquals(good_deployment.getExport().getConfiguration().get(0).getType(), ServerExportEnum.CUSTOM);

        catconn = db.getConnectors().get("bar");
        assertNotNull(catconn);
        assertTrue(good_deployment.getExport().getConfiguration().get(1).isEnabled());
        prop = catconn.getConfig().get(ExportDataProcessor.EXPORT_TO_TYPE);
        assertEquals(prop.getValue(), "org.voltdb.exportclient.ExportToFileClient");
        assertEquals(good_deployment.getExport().getConfiguration().get(1).getType(), ServerExportEnum.FILE);
    }

    public void testDeprecatedExportSyntax() throws Exception {
        if (!MiscUtils.isPro()) { return; } // not supported in community

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
        final String withBadFileExport =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export enabled='true' target='file' >"
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

        VoltCompiler compiler = new VoltCompiler();
        String x[] = {tmpDdl.getAbsolutePath()};

        Catalog cat = compiler.compileCatalogFromDDL(x);
        final File tmpGood = VoltProjectBuilder.writeStringToTempFile(withGoodCustomExport);
        DeploymentType good_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpGood));
        String msg = CatalogUtil.compileDeployment(cat, good_deployment, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        assertTrue(good_deployment.getExport().getConfiguration().get(0).isEnabled());
        assertEquals(good_deployment.getExport().getConfiguration().get(0).getExportconnectorclass(),
                "org.voltdb.exportclient.NoOpTestExportClient");
        assertEquals(good_deployment.getExport().getConfiguration().get(0).getType(), ServerExportEnum.CUSTOM);

        Catalog cat2 = compiler.compileCatalogFromDDL(x);
        final File tmpFileGood = VoltProjectBuilder.writeStringToTempFile(withBadFileExport);
        DeploymentType good_file_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpFileGood));
        msg = CatalogUtil.compileDeployment(cat2, good_file_deployment, false);
        assertTrue("compilation should have failed", msg.contains("ExportToFile: must provide a filename nonce"));

        assertTrue(good_file_deployment.getExport().getConfiguration().get(0).isEnabled());
        assertEquals(good_file_deployment.getExport().getConfiguration().get(0).getType(), ServerExportEnum.FILE);
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
        CatalogUtil.compileDeployment(catalog, tmpDefSchema.getPath(), false);
        Cluster cluster =  catalog.getClusters().get("cluster");
        assertTrue(cluster.getUseddlschema());

        setUp();
        final File tmpCatalogSchema = VoltProjectBuilder.writeStringToTempFile(catalogSchema);
        CatalogUtil.compileDeployment(catalog, tmpCatalogSchema.getPath(), false);
        cluster =  catalog.getClusters().get("cluster");
        assertFalse(cluster.getUseddlschema());

        setUp();
        final File tmpAdhocSchema = VoltProjectBuilder.writeStringToTempFile(adhocSchema);
        CatalogUtil.compileDeployment(catalog, tmpAdhocSchema.getPath(), false);
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
}
