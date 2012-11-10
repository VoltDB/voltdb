/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

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
import org.voltdb.catalog.User;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.ConstraintType;

import com.google.common.base.Joiner;

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
            String sql = CatalogUtil.toSchema(catalog_tbl);
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

    public void testDeploymentCRCs() {
        final String dep1 = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                            "<deployment>" +
                            "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
                            "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                            "<httpd port='0'>" +
                            "<jsonapi enabled='true'/>" +
                            "</httpd>" +
                            "</deployment>";

        // differs in a meaningful way from dep1
        final String dep2 = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                            "<deployment>" +
                            "<cluster hostcount='4' kfactor='1' sitesperhost='2'/>" +
                            "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                            "<httpd port='0'>" +
                            "<jsonapi enabled='true'/>" +
                            "</httpd>" +
                            "</deployment>";

        // differs in whitespace and attribute order from dep1
        final String dep3 = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                            "<deployment>" +
                            "   <cluster hostcount='3' kfactor='1' sitesperhost='2' />" +
                            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                            "   <httpd port='0' >" +
                            "       <jsonapi enabled='true'/>" +
                            "   </httpd>" +
                            "</deployment>";

        // admin-mode section actually impacts CRC, dupe above and add it
        final String dep4 = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                            "<deployment>" +
                            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
                            "   <admin-mode port='32323' adminstartup='true'/>" +
                            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                            "   <httpd port='0' >" +
                            "       <jsonapi enabled='true'/>" +
                            "   </httpd>" +
                            "</deployment>";

        // hearbeat-config section actually impacts CRC, dupe above and add it
        final String dep5 = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                            "<deployment>" +
                            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
                            "       <admin-mode port='32323' adminstartup='true'/>" +
                            "   <heartbeat timeout='30'/>" +
                            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                            "   <httpd port='0' >" +
                            "       <jsonapi enabled='true'/>" +
                            "   </httpd>" +
                            "</deployment>";

        // security section actually impacts CRC, dupe above and add it
        final String dep6 = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                            "<deployment>" +
                            "<security enabled=\"true\"/>" +
                            "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
                            "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                            "<httpd port='0'>" +
                            "<jsonapi enabled='true'/>" +
                            "</httpd>" +
                            "</deployment>";

        // users section actually impacts CRC, dupe above and add it
        final String dep7 = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                            "<deployment>" +
                            "<security enabled=\"true\"/>" +
                            "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
                            "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                            "<httpd port='0'>" +
                            "<jsonapi enabled='true'/>" +
                            "</httpd>" +
                            "<users> " +
                            "<user name=\"joe\" password=\"aaa\" groups=\"louno,lodue\" roles=\"lotre\"/>" +
                            "<user name=\"jone\" password=\"bbb\" groups=\"latre,launo\" roles=\"ladue\"/>" +
                            "</users>" +
                            "</deployment>";

        // users section actually impacts CRC, dupe above
        final String dep8 = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                            "<deployment>" +
                            "<security enabled=\"true\"/>" +
                            "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
                            "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                            "<httpd port='0'>" +
                            "<jsonapi enabled='true'/>" +
                            "</httpd>" +
                            "<users> " +
                            "<user name=\"joe\" password=\"aaa\" roles=\"lotre,lodue,louno\"/>" +
                            "<user name=\"jone\" password=\"bbb\" roles=\"launo,ladue,latre\"/>" +
                            "</users>" +
                            "</deployment>";

        // users section actually impacts CRC, change above
        final String dep9 = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
                            "<deployment>" +
                            "<security enabled=\"true\"/>" +
                            "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
                            "<paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
                            "<httpd port='0'>" +
                            "<jsonapi enabled='true'/>" +
                            "</httpd>" +
                            "<users> " +
                            "<user name=\"joe\" password=\"aaa\" roles=\"lotre,lodue\"/>" +
                            "<user name=\"jone\" password=\"bbb\" roles=\"launo,ladue,latre,laquattro\"/>" +
                            "</users>" +
                            "</deployment>";

        final File tmpDep1 = VoltProjectBuilder.writeStringToTempFile(dep1);
        final File tmpDep2 = VoltProjectBuilder.writeStringToTempFile(dep2);
        final File tmpDep3 = VoltProjectBuilder.writeStringToTempFile(dep3);
        final File tmpDep4 = VoltProjectBuilder.writeStringToTempFile(dep4);
        final File tmpDep5 = VoltProjectBuilder.writeStringToTempFile(dep5);
        final File tmpDep6 = VoltProjectBuilder.writeStringToTempFile(dep6);
        final File tmpDep7 = VoltProjectBuilder.writeStringToTempFile(dep7);
        final File tmpDep8 = VoltProjectBuilder.writeStringToTempFile(dep8);
        final File tmpDep9 = VoltProjectBuilder.writeStringToTempFile(dep9);

        final long crcDep1 = CatalogUtil.getDeploymentCRC(tmpDep1.getPath());
        final long crcDep2 = CatalogUtil.getDeploymentCRC(tmpDep2.getPath());
        final long crcDep3 = CatalogUtil.getDeploymentCRC(tmpDep3.getPath());
        final long crcDep4 = CatalogUtil.getDeploymentCRC(tmpDep4.getPath());
        final long crcDep5 = CatalogUtil.getDeploymentCRC(tmpDep5.getPath());
        final long crcDep6 = CatalogUtil.getDeploymentCRC(tmpDep6.getPath());
        final long crcDep7 = CatalogUtil.getDeploymentCRC(tmpDep7.getPath());
        final long crcDep8 = CatalogUtil.getDeploymentCRC(tmpDep8.getPath());
        final long crcDep9 = CatalogUtil.getDeploymentCRC(tmpDep9.getPath());

        assertTrue(crcDep1 > 0);
        assertTrue(crcDep2 > 0);
        assertTrue(crcDep3 > 0);
        assertTrue(crcDep4 > 0);
        assertTrue(crcDep5 > 0);
        assertTrue(crcDep6 > 0);
        assertTrue(crcDep7 > 0);
        assertTrue(crcDep8 > 0);
        assertTrue(crcDep9 > 0);

        assertTrue(crcDep1 != crcDep2);
        assertTrue(crcDep1 == crcDep3);
        assertTrue(crcDep3 != crcDep4);
        assertTrue(crcDep4 != crcDep5);
        assertTrue(crcDep1 != crcDep6);
        assertTrue(crcDep6 != crcDep7);
        assertTrue(crcDep7 == crcDep8);
        assertTrue(crcDep8 != crcDep9);
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

        long crcDep = CatalogUtil.compileDeploymentAndGetCRC(catalog, tmpDep.getPath(), true);

        assertEquals(30, catalog.getClusters().get("cluster").getHeartbeattimeout());

        // This returns -1 on schema violation
        crcDep = CatalogUtil.compileDeploymentAndGetCRC(catalog, tmpBoom.getPath(), true);
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
        CatalogUtil.compileDeploymentAndGetCRC(catalog, tmpDepOff.getPath(), true);
        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");
        assertFalse(db.getSnapshotschedule().get("default").getEnabled());

        setUp();
        final File tmpDepOn = VoltProjectBuilder.writeStringToTempFile(depOn);
        CatalogUtil.compileDeploymentAndGetCRC(catalog, tmpDepOn.getPath(), true);
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

        final String secOn =
            "<?xml version='1.0' encoding='UTF-8' standalone='no'?>" +
            "<deployment>" +
            "   <cluster hostcount='3' kfactor='1' sitesperhost='2'/>" +
            "   <paths><voltdbroot path=\"/tmp/" + System.getProperty("user.name") + "\" /></paths>" +
            "   <security enabled=\"true\"/>" +
            "</deployment>";

        final File tmpSecOff = VoltProjectBuilder.writeStringToTempFile(secOff);
        CatalogUtil.compileDeploymentAndGetCRC(catalog, tmpSecOff.getPath(), true);
        Cluster cluster =  catalog.getClusters().get("cluster");
        assertFalse(cluster.getSecurityenabled());

        setUp();
        final File tmpSecOn = VoltProjectBuilder.writeStringToTempFile(secOn);
        CatalogUtil.compileDeploymentAndGetCRC(catalog, tmpSecOn.getPath(), true);
        cluster =  catalog.getClusters().get("cluster");
        assertTrue(cluster.getSecurityenabled());
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
            "<user name=\"joe\" password=\"aaa\" roles=\"lotre,lodue,louno\"/>" +
            "<user name=\"jane\" password=\"bbb\" roles=\"launo,ladue,latre\"/>" +
            "</users>" +
            "</deployment>";

        catalog_db.getGroups().add("louno");
        catalog_db.getGroups().add("lodue");
        catalog_db.getGroups().add("lotre");
        catalog_db.getGroups().add("launo");
        catalog_db.getGroups().add("ladue");
        catalog_db.getGroups().add("latre");

        final File tmpRole = VoltProjectBuilder.writeStringToTempFile(depRole);
        CatalogUtil.compileDeploymentAndGetCRC(catalog, tmpRole.getPath(), true);
        Database db = catalog.getClusters().get("cluster")
                .getDatabases().get("database");

        User joe = db.getUsers().get("joe");
        assertNotNull(joe);
        assertNotNull(joe.getGroups().get("louno"));
        assertNotNull(joe.getGroups().get("lodue"));
        assertNotNull(joe.getGroups().get("lotre"));
        assertNull(joe.getGroups().get("latre"));

        User jane = db.getUsers().get("jane");
        assertNotNull(jane);
        assertNotNull(jane.getGroups().get("launo"));
        assertNotNull(jane.getGroups().get("ladue"));
        assertNotNull(jane.getGroups().get("latre"));
        assertNull(jane.getGroups().get("lotre"));
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
        long crcDepOff = CatalogUtil.compileDeploymentAndGetCRC(catalog, tmpDepOff.getPath(), true);
        Systemsettings sysset = catalog.getClusters().get("cluster").getDeployment().get("deployment").getSystemsettings().get("systemsettings");
        assertEquals(100, sysset.getMaxtemptablesize());

        setUp();
        final File tmpDepOn = VoltProjectBuilder.writeStringToTempFile(depOn);
        long crcDepOn = CatalogUtil.compileDeploymentAndGetCRC(catalog, tmpDepOn.getPath(), true);
        sysset = catalog.getClusters().get("cluster").getDeployment().get("deployment").getSystemsettings().get("systemsettings");
        assertEquals(200, sysset.getMaxtemptablesize());
        assertTrue(crcDepOff != crcDepOn);
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
        CatalogUtil.compileDeploymentAndGetCRC(catalog, tmpDeploy.getPath(), true);

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

        CatalogUtil.compileDeploymentAndGetCRC(catalog, depPath, false);

        String commands = catalog.serialize();
        System.out.println(commands);

    }

    public void testCatalogVersionCheck() {
        // really old version shouldn't work
        assertFalse(CatalogUtil.isCatalogCompatible("0.3"));

        // one minor version older than the min supported
        int[] minCompatibleVersion = Arrays.copyOf(CatalogUtil.minCompatibleVersion,
                                                   CatalogUtil.minCompatibleVersion.length);
        for (int i = minCompatibleVersion.length - 1; i >= 0; i--) {
            if (minCompatibleVersion[i] != 0) {
                minCompatibleVersion[i]--;
                break;
            }
        }
        ArrayList<Integer> arrayList = new ArrayList<Integer>();
        for (int part : minCompatibleVersion) {
            arrayList.add(part);
        }
        String version = Joiner.on('.').join(arrayList);
        assertNotNull(version);
        assertFalse(CatalogUtil.isCatalogCompatible(version));

        // one minor version newer than the current version
        final String currentVersion = VoltDB.instance().getVersionString();
        int[] parseCurrentVersion = MiscUtils.parseVersionString(currentVersion);
        parseCurrentVersion[parseCurrentVersion.length - 1]++;
        arrayList = new ArrayList<Integer>();
        for (int part : parseCurrentVersion) {
            arrayList.add(part);
        }
        String futureVersion = Joiner.on('.').join(arrayList);
        assertFalse(CatalogUtil.isCatalogCompatible(futureVersion));

        // longer version string
        String longerVersion = currentVersion + ".2";
        assertFalse(CatalogUtil.isCatalogCompatible(longerVersion));

        // shorter version string
        int[] longVersion = MiscUtils.parseVersionString("2.3.1");
        int[] shortVersion = MiscUtils.parseVersionString("2.3");
        assertEquals(-1, MiscUtils.compareVersions(shortVersion, longVersion));

        // current version should work
        assertTrue(CatalogUtil.isCatalogCompatible(VoltDB.instance().getVersionString()));
    }
}
