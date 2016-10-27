package org.voltdb.regressionsuites;

import java.util.HashSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class JUnit4LocalClusterTest {

    @BeforeClass
    public static void registerInstanceList() {
        VoltServerConfig.setInstanceSet(new HashSet<>());
    }

    @AfterClass
    public static void shutdownClusters() throws InterruptedException {
        VoltServerConfig.shutDownClusters();
    }

}
