/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;


import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
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
import org.voltdb.compilereport.ProcedureAnnotation;
import org.voltdb.types.ConstraintType;

import junit.framework.TestCase;

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
            CatalogSchemaTools.toSchema(sb, catalog_tbl, null, false, null, null);
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

    public void testBadUserPasswordRoles() throws Exception {
        final String badUsername = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "<security enabled=\"true\"/>" +
            "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "<httpd port='0'>" +
            "<jsonapi enabled='true'/>" +
            "</httpd>" +
            "<users> " +
            "<user name=\"fancy pants\" password=\"admin\" roles=\"administrator\"/>" +
            "</users>" +
            "</deployment>";

        final String badPassword = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                "<deployment>" +
                "<security enabled=\"true\"/>" +
                "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
                "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                "<httpd port='0'>" +
                "<jsonapi enabled='true'/>" +
                "</httpd>" +
                "<users> " +
                "<user name=\"fancy$pants\" password=\"ad min\" roles=\"admin:!\"/>" +
                "</users>" +
                "</deployment>";

        final String badRoles = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                "<deployment>" +
                "<security enabled=\"true\"/>" +
                "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
                "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                "<httpd port='0'>" +
                "<jsonapi enabled='true'/>" +
                "</httpd>" +
                "<users> " +
                "<user name=\"fancypants\" password=\"admin\" roles=\"lo uno\"/>" +
                "</users>" +
                "</deployment>";

        File tmpRole = VoltProjectBuilder.writeStringToTempFile(badUsername);
        String res = CatalogUtil.compileDeployment(catalog, tmpRole.getPath(), false);
        assertTrue(res.contains("Error parsing deployment"));

        tmpRole = VoltProjectBuilder.writeStringToTempFile(badPassword);
        res = CatalogUtil.compileDeployment(catalog, tmpRole.getPath(), false);
        assertFalse(res.contains("Error parsing deployment"));

        catalog_db.getGroups().add("lo uno");
        tmpRole = VoltProjectBuilder.writeStringToTempFile(badRoles);
        res = CatalogUtil.compileDeployment(catalog, tmpRole.getPath(), false);
        assertTrue(res.contains("Error parsing deployment"));
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
            "<user name=\"joe\" password=\"D033E22AE348AEB5660FC2140AEC35850C4DA9978C6976E5B5410415BDE908BD4DEE15DFB167A9C873FC4BB8A81F6F2AB448A918\" plaintext=\"false\" roles=\"louno,administrator\"/>" +
            "</users>" +
            "</deployment>";

        catalog_db.getGroups().add("louno");

        final File tmpRole = VoltProjectBuilder.writeStringToTempFile(depRole);

        CatalogUtil.compileDeployment(catalog, tmpRole.getPath(), false);

        Database db = catalog.getClusters().get("cluster")
                .getDatabases().get("database");

        User joe = db.getUsers().get("joe");
        assertNotNull(joe);
        assertNotNull(joe.getGroups().get("louno"));
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
        assertEquals(10000, sysset.getQuerytimeout());

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
            "   <partition-detection enabled='true' />" +
            "</deployment>";

        final String ppdDisabledNoPrefix =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <partition-detection enabled='false' />" +
            "</deployment>";

        final File tmpNoElement = VoltProjectBuilder.writeStringToTempFile(noElement);
        String msg = CatalogUtil.compileDeployment(catalog, tmpNoElement.getPath(), false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);
        Cluster cluster = catalog.getClusters().get("cluster");
        assertTrue(cluster.getNetworkpartition());

        setUp();
        final File tmpEnabledDefault = VoltProjectBuilder.writeStringToTempFile(ppdEnabledDefaultPrefix);
        msg = CatalogUtil.compileDeployment(catalog, tmpEnabledDefault.getPath(), false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);
        cluster = catalog.getClusters().get("cluster");
        assertTrue(cluster.getNetworkpartition());

        setUp();
        final File tmpDisabled = VoltProjectBuilder.writeStringToTempFile(ppdDisabledNoPrefix);
        msg = CatalogUtil.compileDeployment(catalog, tmpDisabled.getPath(), false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);
        cluster = catalog.getClusters().get("cluster");
        assertFalse(cluster.getNetworkpartition());
    }

    public void testImportSettings() throws Exception {

        File formatjar = CatalogUtil.createTemporaryEmptyCatalogJarFile(false);
        final String withBadImport1 =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <import>"
                + "        <configuration type=\"custom\" module=\"///\" "
                + "format=\"file:" + formatjar.toString() + "/csv\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </import>"
                + "</deployment>";
        final String withBadImport2 =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <import>"
                + "        <configuration type=\"custom\" module=\"file:/tmp/foobar.jar\" "
                + "format=\"file:" + formatjar.toString() + "/csv\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </import>"
                + "</deployment>";
        //Use catalog jar to point to a file to do dup test.
        File catjar = CatalogUtil.createTemporaryEmptyCatalogJarFile(false);
        final String withBadImport3 =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <import>"
                + "        <configuration type=\"custom\" module=\"file:/" + catjar.toString()
                + "\" format=\"file:" + formatjar.toString() + "/csv\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "        <configuration type=\"custom\" module=\"file:/" + catjar.toString()
                + "\" format=\"file:" + formatjar.toString() + "/csv\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </import>"
                + "</deployment>";
        final String withGoodImport0 =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <import>"
                + "        <configuration type=\"custom\" module=\"file:" + catjar.toString()
                + "\" format=\"file:" + formatjar.toString() + "/csv\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "        <configuration type=\"custom\" module=\"file:" + catjar.toString()
                + "\" format=\"file:" + formatjar.toString() + "/csv\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </import>"
                + "</deployment>";
        final String goodImport1 =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <import>"
                + "        <configuration type=\"custom\" module=\"file:" + catjar.toString()
                + "\" format=\"file:" + formatjar.toString() + "/csv\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </import>"
                + "</deployment>";
        final String withBadFormatter1 =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <import>"
                + "        <configuration type=\"custom\" module=\"file:" + catjar.toString() + "\" format=\"badformatter.jar/csv\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </import>"
                + "</deployment>";


        final String ddl =
                "CREATE TABLE data ( id BIGINT default 0 , value BIGINT DEFAULT 0 );\n";

        final File tmpDdl = VoltProjectBuilder.writeStringToTempFile(ddl);

        //import with bad bundlename
        final File tmpBad = VoltProjectBuilder.writeStringToTempFile(withBadImport1);
        DeploymentType bad_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpBad));

        VoltCompiler compiler = new VoltCompiler(false);
        String x[] = {tmpDdl.getAbsolutePath()};
        Catalog cat = compiler.compileCatalogFromDDL(x);

        String msg = CatalogUtil.compileDeployment(cat, bad_deployment, false);
        assertTrue(msg, msg.contains("Error validating deployment configuration: Import failed to configure, failed to load module by URL or classname provided"));

        //import with bad bundlename
        final File tmpBad2 = VoltProjectBuilder.writeStringToTempFile(withBadImport2);
        DeploymentType bad_deployment2 = CatalogUtil.getDeployment(new FileInputStream(tmpBad2));

        VoltCompiler compiler2 = new VoltCompiler(false);
        String x2[] = {tmpDdl.getAbsolutePath()};
        Catalog cat2 = compiler2.compileCatalogFromDDL(x2);

        String msg2 = CatalogUtil.compileDeployment(cat2, bad_deployment2, false);
        assertTrue("compilation should have failed", msg2.contains("Error validating deployment configuration: Import failed to configure, failed to load module by URL or classname provided"));

        //import with bad url for bundlename
        final File tmpBad3 = VoltProjectBuilder.writeStringToTempFile(withBadImport3);
        DeploymentType bad_deployment3 = CatalogUtil.getDeployment(new FileInputStream(tmpBad3));

        VoltCompiler compiler3 = new VoltCompiler(false);
        String x3[] = {tmpDdl.getAbsolutePath()};
        Catalog cat3 = compiler3.compileCatalogFromDDL(x3);

        String msg3 = CatalogUtil.compileDeployment(cat3, bad_deployment3, false);
        assertTrue("compilation should have failed", msg3.contains("Error validating deployment configuration: Import failed to configure, failed to load module by URL or classname provided"));

        //import with dup should be ok now
        final File tmpBad4 = VoltProjectBuilder.writeStringToTempFile(withGoodImport0);
        DeploymentType bad_deployment4 = CatalogUtil.getDeployment(new FileInputStream(tmpBad4));

        VoltCompiler compiler4 = new VoltCompiler(false);
        String x4[] = {tmpDdl.getAbsolutePath()};
        Catalog cat4 = compiler4.compileCatalogFromDDL(x4);

        String msg4 = CatalogUtil.compileDeployment(cat4, bad_deployment4, false);
        assertNull(msg4);

        //import good bundle not necessary loadable by felix.
        final File good1 = VoltProjectBuilder.writeStringToTempFile(goodImport1);
        DeploymentType good_deployment1 = CatalogUtil.getDeployment(new FileInputStream(good1));

        VoltCompiler good_compiler1 = new VoltCompiler(false);
        String x5[] = {tmpDdl.getAbsolutePath()};
        Catalog cat5 = good_compiler1.compileCatalogFromDDL(x5);

        String msg5 = CatalogUtil.compileDeployment(cat5, good_deployment1, false);
        assertNull(msg5);

        //formatter with invalid jar
        final File tmpBad5 = VoltProjectBuilder.writeStringToTempFile(withBadFormatter1);
        DeploymentType bad_deployment5 = CatalogUtil.getDeployment(new FileInputStream(tmpBad5));

        VoltCompiler compiler6 = new VoltCompiler(false);
        String x6[] = {tmpDdl.getAbsolutePath()};
        Catalog cat6 = compiler6.compileCatalogFromDDL(x6);

        String msg6 = CatalogUtil.compileDeployment(cat6, bad_deployment5, false);
        assertTrue("compilation should have failed", msg6.contains("Error validating deployment configuration: Import failed to configure, failed to load module by URL or classname provided"));
        System.out.println("Import deployment tests done.");
    }

    /**
     * The CRC of an empty catalog should always be the same.
     */
    public void testEmptyCatalogCRC() throws Exception {
        File file1 = CatalogUtil.createTemporaryEmptyCatalogJarFile(false);
        assertNotNull(file1);
        byte[] bytes1 = MiscUtils.fileToBytes(file1);
        InMemoryJarfile jar1 = new InMemoryJarfile(bytes1);
        long crc1 = jar1.getCRC();
        Thread.sleep(5000);
        File file2 = CatalogUtil.createTemporaryEmptyCatalogJarFile(false);
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

    public void testDRRole() throws Exception {
        final String defaultRole =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
            + "<deployment>"
            + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
            + "    <dr id='1'>"
            + "    </dr>"
            + "</deployment>";
        final String masterRole =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
            + "<deployment>"
            + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
            + "    <dr id='1' role='master'>"
            + "    </dr>"
            + "</deployment>";
        final String replicaRole =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
            + "<deployment>"
            + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
            + "    <dr id='1' role='replica'>"
            + "    </dr>"
            + "</deployment>";
        final String xdcrRole =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
            + "<deployment>"
            + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
            + "    <dr id='1' role='xdcr'>"
            + "    </dr>"
            + "</deployment>";
        final String invalidRole =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
            + "<deployment>"
            + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
            + "    <dr id='1' role='nonsense'>"
            + "    </dr>"
            + "</deployment>";


        final File tmpDefault = VoltProjectBuilder.writeStringToTempFile(defaultRole);
        DeploymentType defaultDeployment = CatalogUtil.getDeployment(new FileInputStream(tmpDefault));
        CatalogUtil.compileDeployment(catalog, defaultDeployment, false);
        assertEquals("master", catalog.getClusters().get("cluster").getDrrole());

        setUp();
        final File tmpMaster = VoltProjectBuilder.writeStringToTempFile(masterRole);
        DeploymentType masterDeployment = CatalogUtil.getDeployment(new FileInputStream(tmpMaster));
        CatalogUtil.compileDeployment(catalog, masterDeployment, false);
        assertEquals("master", catalog.getClusters().get("cluster").getDrrole());

        setUp();
        final File tmpReplica = VoltProjectBuilder.writeStringToTempFile(replicaRole);
        DeploymentType replicaDeployment = CatalogUtil.getDeployment(new FileInputStream(tmpReplica));
        CatalogUtil.compileDeployment(catalog, replicaDeployment, false);
        assertEquals("replica", catalog.getClusters().get("cluster").getDrrole());

        setUp();
        final File tmpXDCR = VoltProjectBuilder.writeStringToTempFile(xdcrRole);
        DeploymentType xdcrDeployment = CatalogUtil.getDeployment(new FileInputStream(tmpXDCR));
        CatalogUtil.compileDeployment(catalog, xdcrDeployment, false);
        assertEquals("xdcr", catalog.getClusters().get("cluster").getDrrole());

        setUp();
        final File tmpInvalidRole = VoltProjectBuilder.writeStringToTempFile(invalidRole);
        assertNull(CatalogUtil.getDeployment(new FileInputStream(tmpInvalidRole)));
    }

    public void testDRConnection() throws Exception {
        final String multipleConnections =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
                + "    <dr>"
                + "        <connection source='master'/>"
                + "        <connection source='imposter'/>"
                + "    </dr>"
                + "</deployment>";
        final String oneConnection =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
                + "    <dr>"
                + "        <connection source='master'/>"
                + "    </dr>"
                + "</deployment>";
        final String oneEnabledConnection =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
                + "    <dr>"
                + "        <connection source='master'/>"
                + "    </dr>"
                + "</deployment>";
        final String drDisabled =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
                + "    <dr listen='false'>"
                + "        <connection source='master'/>"
                + "    </dr>"
                + "</deployment>";
        final String clusterIdTooSmall =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='-1'/>"
                + "    <dr>"
                + "        <connection source='master'/>"
                + "    </dr>"
                + "</deployment>";
        final String clusterIdTooLarge =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='128'/>"
                + "    <dr>"
                + "        <connection source='master'/>"
                + "    </dr>"
                + "</deployment>";
        final String twoClusterIds =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='5'/>"
                + "    <dr id='5'>"
                + "        <connection source='master'/>"
                + "    </dr>"
                + "</deployment>";
        final String twoConflictingClusterIds =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='5'/>"
                + "    <dr id='2'>"
                + "        <connection source='master'/>"
                + "    </dr>"
                + "</deployment>";
        final String drEnabledNoConnection =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='0'/>"
                + "    <dr listen='true'>"
                + "    </dr>"
                + "</deployment>";
        final String drEnabledWithEnabledConnection =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
                + "    <dr listen='true'>"
                + "        <connection source='master'/>"
                + "    </dr>"
                + "</deployment>";
        final String drEnabledWithPort =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2' id='1'/>"
                + "    <dr listen='true' port='100'>"
                + "        <connection source='master'/>"
                + "    </dr>"
                + "</deployment>";

        final File tmpInvalidMultiple = VoltProjectBuilder.writeStringToTempFile(multipleConnections);
        assertNull(CatalogUtil.getDeployment(new FileInputStream(tmpInvalidMultiple)));

        final File tmpLowClusterId = VoltProjectBuilder.writeStringToTempFile(clusterIdTooSmall);
        assertNull(CatalogUtil.getDeployment(new FileInputStream(tmpLowClusterId)));

        final File tmpHighClusterId = VoltProjectBuilder.writeStringToTempFile(clusterIdTooLarge);
        assertNull(CatalogUtil.getDeployment(new FileInputStream(tmpHighClusterId)));

        assertTrue(catalog.getClusters().get("cluster").getDrmasterhost().isEmpty());
        assertFalse(catalog.getClusters().get("cluster").getDrproducerenabled());
        assertTrue(catalog.getClusters().get("cluster").getDeployment().isEmpty());

        final File tmpDefault = VoltProjectBuilder.writeStringToTempFile(oneConnection);
        DeploymentType valid_deployment = CatalogUtil.getDeployment(new FileInputStream(tmpDefault));
        assertNotNull(valid_deployment);

        String msg = CatalogUtil.compileDeployment(catalog, valid_deployment, false);
        assertTrue("Deployment file failed to parse", msg == null);

        assertEquals("master", catalog.getClusters().get("cluster").getDrmasterhost());
        assertTrue(catalog.getClusters().get("cluster").getDrproducerenabled());
        assertTrue(catalog.getClusters().get("cluster").getDrclusterid() == 1);

        final File tmpEnabled = VoltProjectBuilder.writeStringToTempFile(oneEnabledConnection);
        DeploymentType valid_deployment_enabled = CatalogUtil.getDeployment(new FileInputStream(tmpEnabled));
        assertNotNull(valid_deployment_enabled);

        setUp();
        msg = CatalogUtil.compileDeployment(catalog, valid_deployment_enabled, false);
        assertTrue("Deployment file failed to parse", msg == null);

        assertEquals("master", catalog.getClusters().get("cluster").getDrmasterhost());
        assertTrue(catalog.getClusters().get("cluster").getDrproducerenabled());
        assertTrue(catalog.getClusters().get("cluster").getDrclusterid() == 1);

        final File tmpDisabled = VoltProjectBuilder.writeStringToTempFile(drDisabled);
        DeploymentType valid_deployment_disabled = CatalogUtil.getDeployment(new FileInputStream(tmpDisabled));
        assertNotNull(valid_deployment_disabled);

        setUp();
        msg = CatalogUtil.compileDeployment(catalog, valid_deployment_disabled, false);
        assertTrue("Deployment file failed to parse", msg == null);

        assertFalse(catalog.getClusters().get("cluster").getDrmasterhost().isEmpty());
        assertFalse(catalog.getClusters().get("cluster").getDrproducerenabled());
        assertTrue(catalog.getClusters().get("cluster").getDrclusterid() == 1);

        final File tmpEnabledNoConn = VoltProjectBuilder.writeStringToTempFile(drEnabledNoConnection);
        DeploymentType valid_deployment_enabledNoConn = CatalogUtil.getDeployment(new FileInputStream(tmpEnabledNoConn));
        assertNotNull(valid_deployment_enabledNoConn);

        setUp();
        msg = CatalogUtil.compileDeployment(catalog, valid_deployment_enabledNoConn, false);
        assertTrue("Deployment file failed to parse", msg == null);

        assertTrue(catalog.getClusters().get("cluster").getDrmasterhost().isEmpty());
        assertTrue(catalog.getClusters().get("cluster").getDrproducerenabled());
        assertTrue(catalog.getClusters().get("cluster").getDrclusterid() == 0);

        final File tmpTwoClusterIds = VoltProjectBuilder.writeStringToTempFile(twoClusterIds);
        DeploymentType valid_deployment_twoClusterIds = CatalogUtil.getDeployment(new FileInputStream(tmpTwoClusterIds));
        assertNotNull(valid_deployment_twoClusterIds);

        setUp();
        msg = CatalogUtil.compileDeployment(catalog, valid_deployment_twoClusterIds, false);
        assertTrue("Deployment file failed to parse", msg == null);

        final File tmpTwoConflictingClusterIds = VoltProjectBuilder.writeStringToTempFile(twoConflictingClusterIds);
        DeploymentType invalid_deployment_twoConflictingClusterIds = CatalogUtil.getDeployment(new FileInputStream(tmpTwoConflictingClusterIds));
        assertNotNull(invalid_deployment_twoConflictingClusterIds);

        setUp();
        msg = CatalogUtil.compileDeployment(catalog, invalid_deployment_twoConflictingClusterIds, false);
        assertTrue("Deployment file failed to parse", msg != null);

        final File tmpEnabledWithConn = VoltProjectBuilder.writeStringToTempFile(drEnabledWithEnabledConnection);
        DeploymentType valid_deployment_enabledWithConn = CatalogUtil.getDeployment(new FileInputStream(tmpEnabledWithConn));
        assertNotNull(valid_deployment_enabledWithConn);

        setUp();
        msg = CatalogUtil.compileDeployment(catalog, valid_deployment_enabledWithConn, false);
        assertTrue("Deployment file failed to parse", msg == null);

        assertEquals("master", catalog.getClusters().get("cluster").getDrmasterhost());
        assertTrue(catalog.getClusters().get("cluster").getDrproducerenabled());
        assertTrue(catalog.getClusters().get("cluster").getDrclusterid() == 1);

        final File tmpEnabledWithPort = VoltProjectBuilder.writeStringToTempFile(drEnabledWithPort);
        DeploymentType valid_deployment_port = CatalogUtil.getDeployment(new FileInputStream(tmpEnabledWithPort));
        assertNotNull(valid_deployment_port);

        setUp();
        msg = CatalogUtil.compileDeployment(catalog, valid_deployment_port, false);
        assertTrue("Deployment file failed to parse", msg == null);

        assertFalse(catalog.getClusters().get("cluster").getDrmasterhost().isEmpty());
        assertTrue(catalog.getClusters().get("cluster").getDrproducerenabled());
        assertTrue(catalog.getClusters().get("cluster").getDrproducerport() == 100);
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
                               "DR TABLE A; DR TABLE B; DR TABLE C;\n",
                               "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "DR TABLE A; DR TABLE B; DR TABLE C;\n");

        // Missing one table
        verifyDrTableSignature(false,
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                               "DR TABLE A; DR TABLE B; DR TABLE C;\n",
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "DR TABLE A; DR TABLE B;\n");

        // Different column type
        verifyDrTableSignature(false,
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 FLOAT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                               "DR TABLE A; DR TABLE B; DR TABLE C;\n",
                               "CREATE TABLE C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL); PARTITION TABLE C ON COLUMN C1;\n" +
                               "CREATE TABLE A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL); PARTITION TABLE A ON COLUMN C1;\n" +
                               "CREATE TABLE B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL); PARTITION TABLE B ON COLUMN C1;\n" +
                               "DR TABLE A; DR TABLE B; DR TABLE C;\n");
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
                                           "DR TABLE A; DR TABLE B; DR TABLE C;\n",
                                           Pair.of("A", "ip"),
                                           Pair.of("B", "bs"),
                                           Pair.of("C", "tv"));
    }

    public void testJSONAPIFlag() throws Exception
    {
        final String noHTTPElement =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                "<deployment>" +
                "   <cluster hostcount='3' kfactor='1' sitesperhost='2' />" +
                "</deployment>";

        final String noJSONAPIElement =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                "<deployment>" +
                "   <cluster hostcount='3' kfactor='1' sitesperhost='2' />" +
                "   <httpd port='0' />" +
                "</deployment>";

        final String jsonAPITrue =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                "<deployment>" +
                "   <cluster hostcount='3' kfactor='1' sitesperhost='2' />" +
                "   <httpd port='0'>" +
                "      <jsonapi enabled='true' />" +
                "   </httpd>" +
                "</deployment>";

        final String jsonAPIFalse =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                "<deployment>" +
                "   <cluster hostcount='3' kfactor='1' sitesperhost='2' />" +
                "   <httpd port='0'>" +
                "      <jsonapi enabled='false' />" +
                "   </httpd>" +
                "</deployment>";

        File tmp = VoltProjectBuilder.writeStringToTempFile(noHTTPElement);
        CatalogUtil.compileDeployment(catalog, tmp.getPath(), false);
        Cluster cluster =  catalog.getClusters().get("cluster");
        assertTrue(cluster.getJsonapi());

        setUp();
        tmp = VoltProjectBuilder.writeStringToTempFile(noJSONAPIElement);
        CatalogUtil.compileDeployment(catalog, tmp.getPath(), false);
        cluster =  catalog.getClusters().get("cluster");
        assertTrue(cluster.getJsonapi());

        setUp();
        tmp = VoltProjectBuilder.writeStringToTempFile(jsonAPITrue);
        CatalogUtil.compileDeployment(catalog, tmp.getPath(), false);
        cluster =  catalog.getClusters().get("cluster");
        assertTrue(cluster.getJsonapi());

        setUp();
        tmp = VoltProjectBuilder.writeStringToTempFile(jsonAPIFalse);
        CatalogUtil.compileDeployment(catalog, tmp.getPath(), false);
        cluster =  catalog.getClusters().get("cluster");
        assertFalse(cluster.getJsonapi());
    }

    public void testMemoryLimitNegative() throws Exception {
        final String deploymentString =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "  <cluster hostcount=\"1\" kfactor=\"0\" />"
                + "  <httpd enabled=\"true\">"
                + "    <jsonapi enabled=\"true\" />"
                + "  </httpd>"
                + "  <systemsettings>"
                + "    <resourcemonitor>"
                + "      <memorylimit size=\"90.5%\"/>"
                + "    </resourcemonitor>"
                + "  </systemsettings>"
                + "</deployment>";
        final String ddl =
                 "CREATE TABLE T (D1 INTEGER NOT NULL, D2 INTEGER);\n";
        final File tmpWithDefault = VoltProjectBuilder.writeStringToTempFile(deploymentString);
        DeploymentType deploymentWithDefault = CatalogUtil.getDeployment(new FileInputStream(tmpWithDefault));

        final File tmpDdl = VoltProjectBuilder.writeStringToTempFile(ddl);
        VoltCompiler compiler = new VoltCompiler(false);
        String x[] = {tmpDdl.getAbsolutePath()};
        Catalog cat = compiler.compileCatalogFromDDL(x);

        String msg = CatalogUtil.compileDeployment(cat, deploymentWithDefault, false);
        assertTrue(msg.contains("Invalid memory limit"));
    }

    public void testDiskLimitNegative() throws Exception {
        final String deploymentString =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "  <cluster hostcount=\"1\" kfactor=\"0\" />"
                + "  <httpd enabled=\"true\">"
                + "    <jsonapi enabled=\"true\" />"
                + "  </httpd>"
                + "  <systemsettings>"
                + "    <resourcemonitor>"
                + "      <disklimit>"
                + "        <feature name=\"commandlog\" size=\"xx\"/>"
                + "      </disklimit>"
                + "    </resourcemonitor>"
                + "  </systemsettings>"
                + "</deployment>";
        final String ddl =
                 "CREATE TABLE T (D1 INTEGER NOT NULL, D2 INTEGER);\n";
        final File tmpWithDefault = VoltProjectBuilder.writeStringToTempFile(deploymentString);
        DeploymentType deploymentWithDefault = CatalogUtil.getDeployment(new FileInputStream(tmpWithDefault));

        final File tmpDdl = VoltProjectBuilder.writeStringToTempFile(ddl);
        VoltCompiler compiler = new VoltCompiler(false);
        String x[] = {tmpDdl.getAbsolutePath()};
        Catalog cat = compiler.compileCatalogFromDDL(x);

        String msg = CatalogUtil.compileDeployment(cat, deploymentWithDefault, false);
        assertTrue(msg.contains("Invalid value"));
    }

    public void testGetNormalTableNamesFromInMemoryJar() throws Exception {
        String schema = "CREATE TABLE NORMAL_A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL);\n" +
                "CREATE TABLE NORMAL_B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL);\n" +
                "CREATE TABLE NORMAL_C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL);\n" +
                "CREATE VIEW VIEW_A (TOTAL_ROWS) AS SELECT COUNT(*) FROM NORMAL_A;\n" +
                "CREATE STREAM EXPORT_A (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL);\n";
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        final File file = VoltFile.createTempFile("testGetNormalTableNamesFromInMemoryJar", ".jar", new File(testDir));

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(schema);
        builder.compile(file.getPath());
        byte[] bytes = MiscUtils.fileToBytes(file);
        InMemoryJarfile jarfile = CatalogUtil.loadInMemoryJarFile(bytes);
        file.delete();

        Set<String> definedNormalTableNames = new HashSet<>();
        definedNormalTableNames.add("NORMAL_A");
        definedNormalTableNames.add("NORMAL_B");
        definedNormalTableNames.add("NORMAL_C");
        Set<String> returnedNormalTableNames = CatalogUtil.getNormalTableNamesFromInMemoryJar(jarfile);
        assertEquals(definedNormalTableNames, returnedNormalTableNames);
    }
}
