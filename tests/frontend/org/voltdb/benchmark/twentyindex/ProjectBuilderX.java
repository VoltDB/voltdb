package org.voltdb.benchmark.twentyindex;

import java.net.URL;
import org.voltdb.compiler.VoltProjectBuilder;

import org.voltdb.benchmark.twentyindex.procedures.Insert;

public class ProjectBuilderX extends VoltProjectBuilder {

    public static final Class<?> m_procedures[] = new Class<?>[] {
        Insert.class,
        };
    
    public static final Class<?> m_supplementalClasses[] = new Class<?>[] {
        ClientBenchmark.class,
        ProjectBuilderX.class
    };
    
    public static final URL m_ddlURL = ProjectBuilderX.class.getResource("ddl.sql");
    
    public static String m_partitioning[][] = new String[][] {
        {"TABLE1", "MAINID"},
        {"TABLE2", "MAINID"},
        {"TABLE3", "MAINID"},
    };
    
    public void addAllDefaults() {
        addProcedures(m_procedures);
        for (String partitionInfo[] : m_partitioning) {
            addPartitionInfo(partitionInfo[0], partitionInfo[1]);
        }
        addSchema(m_ddlURL);
        addSupplementalClasses(m_supplementalClasses);
    }
}
