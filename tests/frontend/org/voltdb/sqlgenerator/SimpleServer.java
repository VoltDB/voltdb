/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.sqlgenerator;

import java.util.ArrayList;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;
import org.voltdb.regressionsuites.LocalCluster;

public class SimpleServer {
    public static void main(String[] args) {
        SimpleProjectBuilder builder = null;
        String[] arg;
        ArrayList<String> host_manager_args = new ArrayList<String>();
        int hosts = 1;
        int sites = 1;
        int k_factor = 0;

        for (int i=0; i < args.length; ++i) {
            arg = args[i].split("=");
            if (arg[0].equals("schema")) {
                builder = new SimpleProjectBuilder(arg[1]);
            } else if (arg[0].equals("backend")) {
                host_manager_args.add(arg[1]);
            }
            else if (arg[0].equals("hosts"))
            {
                hosts = Integer.valueOf(arg[1]);
            }
            else if (arg[0].equals("sitesperhost"))
            {
                sites = Integer.valueOf(arg[1]);
            }
            else if (arg[0].equals("replicas"))
            {
                k_factor = Integer.valueOf(arg[1]);
            }
            else if (args[i] != null)
            {
                host_manager_args.add(args[i]);
            }
        }

        String[] hostargs = new String[host_manager_args.size()];

        VoltDB.Configuration config =
            new VoltDB.Configuration(host_manager_args.toArray(hostargs));
        config.setPathToCatalogForTest("simple.jar");
        if (config.m_backend != BackendTarget.NATIVE_EE_JNI)
        {
            sites = 1;
            hosts = 1;
            k_factor = 0;
        }
        if (builder == null) {
            builder = new SimpleProjectBuilder("DDL.sql");
        }

        builder.addDefaultSchema();
        builder.addDefaultProcedures();
        builder.addDefaultPartitioning();
        builder.setCompilerDebugPrintStream(System.out);

        System.out.println("config path: " + config.m_pathToCatalog);

        LocalCluster cluster = new LocalCluster("simple.jar",
                                                sites, hosts, k_factor,
                                                config.m_backend);

        if (!cluster.compile(builder))
        {
            System.err.println("Compilation failed");
            System.exit(-1);
        }

        cluster.startUp();

        //VoltDB.initialize(config);
        //VoltDB.instance().run();
    }

}
