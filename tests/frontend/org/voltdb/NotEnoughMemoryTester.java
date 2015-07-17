package org.voltdb;

import java.io.IOException;

import org.voltcore.utils.PortGenerator;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

import junit.framework.TestCase;
// import testFromFile ? (or change to import random stuff)

public class NotEnoughMemoryTester extends TestCase {
    
    // test final vars here
    
    public static void setUpSchema (VoltProjectBuilder builder,
                                    String pathToCatalog,
                                    String pathToDeployment) throws Exception {
        String schema = "CREATE TABLE info ("
                + "partid BIGINT NOT NULL,"
                + "dat VARCHAR(10000),"
                + "PRIMARY KEY (partid)"
                + ");"
                + "PARTITION TABLE info ON COLUMN partid;"
                
                + "CREATE PROCEDURE readinfo "
                + "PARTITION ON TABLE info COLUMN partid "
                + "AS "
                + "SELECT partid,dat FROM info "
                + "WHERE partid=?;"
                
                + "LOAD CLASSES storedprocs.jar;"
                
                + "CREATE PROCEDURE "
                + "PARTITION ON TABLE info COLUMN partid "
                + "FROM CLASS WriteFromFile;";
        
        builder.addLiteralSchema(schema);
        
    }
    
    public static VoltDB.Configuration setUpSPDB() throws IOException, Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocsp.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocsp.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();

        setUpSchema(builder, pathToCatalog, pathToDeployment);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration(new PortGenerator());
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        return config;
    }
}
