package org.voltdb.benchmark.workloads;

import java.net.URL;

import org.voltdb.benchmark.workloads.*;
import org.voltdb.benchmark.workloads.procedures.*;

import java.io.*;

import org.voltdb.compiler.VoltProjectBuilder;

public class ProjectBuilderX extends VoltProjectBuilder
{
    public static final Class<?> m_procedures[] = new Class<?>[]
    {
        Delete.class,
        Insert.class,
        Select.class,
        SelectAll.class
    };

    public static final Class<?> m_supplementalClasses[] = new Class<?>[]
    {
        Generator.class,
        ProjectBuilderX.class
    };

    public static final URL m_ddlURL = ProjectBuilderX.class.getResource("MiniBenchmark-ddl.sql");

    public static String m_partitioning[][] = new String[][]
    {
        {"MINIBENCHMARK", "MINIBENCHMARK_ID"}
    };

    @Override
    public void addAllDefaults()
    {
        addProcedures(m_procedures);
        for (String partitionInfo[] : m_partitioning) {
            addPartitionInfo(partitionInfo[0], partitionInfo[1]);
        }
        addSchema(m_ddlURL);
        addSupplementalClasses(m_supplementalClasses);
    }
}
