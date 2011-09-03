/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.util.HashMap;

import org.voltdb.catalog.Procedure;


/**
 * Lists built-in system stored procedures with metadata
 */
public class SystemProcedureCatalog {

    // Historical note:

    // We used to list syprocs in the catalog (inserting them in
    // VoltCompiler). That adds unnecessary content to catalogs,
    // couples catalogs to versions (an old catalog wouldn't be able
    // to invoke a new sysprocs), and complicates the idea of
    // commercial-only sysprocs.

    // Now we maintain this list here in code. This is also not
    // roses - as ProcedureWrapper really wants catalog.Procedures
    // and ClientInterface has to check two lists at dispatch
    // time.

    /* Data about each registered procedure */
    public static class Config {
        public final String className;
        public final boolean readOnly;
        public final boolean everySite;
        public final boolean commercial;

        public Config(String className,
                boolean readOnly,
                boolean everySite,
                boolean commercial)
        {
            this.className = className;
            this.readOnly = readOnly;
            this.everySite = everySite;
            this.commercial = commercial;
        }

        boolean getEverysite() {
            return everySite;
        }

        boolean getReadonly() {
            return readOnly;
        }

        boolean getSinglepartition() {
            return className.equals("org.voltdb.sysprocs.UpdateApplicationCatalog");
        }

        String getClassname() {
            return className;
        }

        Procedure asCatalogProcedure() {
            Procedure p = new Procedure();
            p.setClassname(className);
            p.setReadonly(readOnly);
            p.setEverysite(everySite);
            p.setSinglepartition(false);
            p.setSystemproc(true);
            p.setHasjava(true);
            p.setPartitiontable(null);
            p.setPartitioncolumn(null);
            p.setPartitionparameter(0);
            return p;
        }
    }

    public static final HashMap<String, Config> listing =
        new HashMap<String, Config>();

    static {
        listing.put("@AdHoc",                   new Config("org.voltdb.sysprocs.AdHoc",             false, false, false));
        listing.put("@AdHocSP",                 new Config("org.voltdb.sysprocs.AdHocSP",           false, false, false));
        listing.put("@Pause",                   new Config("org.voltdb.sysprocs.Pause",             false, true,  false));
        listing.put("@Resume",                  new Config("org.voltdb.sysprocs.Resume",            false, true,  false));
        listing.put("@Quiesce",                 new Config("org.voltdb.sysprocs.Quiesce",           false, false, false));
        listing.put("@Rejoin",                  new Config("org.voltdb.sysprocs.Rejoin",            false, false, false));
        listing.put("@SnapshotSave",            new Config("org.voltdb.sysprocs.SnapshotSave",      false, false, false));
        listing.put("@SnapshotRestore",         new Config("org.voltdb.sysprocs.SnapshotRestore",   false, false, false));
        listing.put("@SnapshotStatus",          new Config("org.voltdb.sysprocs.SnapshotStatus",    false, false, false));
        listing.put("@SnapshotScan",            new Config("org.voltdb.sysprocs.SnapshotScan",      false, false, false));
        listing.put("@SnapshotDelete",          new Config("org.voltdb.sysprocs.SnapshotDelete",    false, false, false));
        listing.put("@Shutdown",                new Config("org.voltdb.sysprocs.Shutdown",          false, false, false));
        listing.put("@ProfCtl",                 new Config("org.voltdb.sysprocs.ProfCtl",           false, false, false));
        listing.put("@Statistics",              new Config("org.voltdb.sysprocs.Statistics",        true,  false, false));
        listing.put("@SystemCatalog",           new Config("org.voltdb.sysprocs.SystemCatalog",     true,  false, false));
        listing.put("@SystemInformation",       new Config("org.voltdb.sysprocs.SystemInformation", true,  false, false));
        listing.put("@UpdateLogging",           new Config("org.voltdb.sysprocs.UpdateLogging",     false, true,  false));
        listing.put("@BalancePartitions",       new Config("org.voltdb.sysprocs.BalancePartitions", false, false, true));
        listing.put("@UpdateApplicationCatalog",new Config("org.voltdb.sysprocs.UpdateApplicationCatalog", false, true,  false));
        listing.put("@LoadMultipartitionTable", new Config("org.voltdb.sysprocs.LoadMultipartitionTable",  false, false, false));
    }

}
