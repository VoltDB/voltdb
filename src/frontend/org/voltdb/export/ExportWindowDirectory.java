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

package org.voltdb.export;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.VoltFile;

/**
 *  Provides an interface to list, claim, unclaim, create and delete
 *  ExportWindows.
 */
public class ExportWindowDirectory {

    // Map generation id to generation.
    public final AtomicReference<TreeMap<Long, ExportGeneration>> m_windows =
            new AtomicReference<TreeMap<Long, ExportGeneration>>(new TreeMap<Long, ExportGeneration>());

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    // user-configured export overflow directory
    private final String m_overflowPath;
    private final File m_exportOverflowDirectory;

    /**
     * Configure a new ExportWindowDirectory
     * @param context
     * @throws IOException
     */
    public ExportWindowDirectory(boolean isRejoin, CatalogContext context) throws IOException
    {
        m_overflowPath = context.cluster.getExportoverflow();
        m_exportOverflowDirectory = new File(m_overflowPath);

        // If a node is rejoining it is because it crashed. Export overflow isn't crash
        // safe. It isn't possible to recover valid/consistent data. Delete it instead.
        if (isRejoin) {
            deletePersistedWindows();
        }
    }


    /**
     * Remove all on-disk ExportWindows or die trying.
     */
    public void deletePersistedWindows()
    {
        File exportOverflowDirectory = new File(m_overflowPath);
        exportLog.info("Deleting export overflow data from " + exportOverflowDirectory);

        if (!exportOverflowDirectory.exists()) {
            return;
        }

        File files[] = exportOverflowDirectory.listFiles();
        if (files != null) {
            for (File f : files) {
                try {
                    VoltFile.recursivelyDelete(f);
                } catch (IOException e) {
                    exportLog.fatal(e);
                    VoltDB.crashLocalVoltDB("Failed to remove overflow directory", false, e);
                }
            }
        }
    }

    /**
     * Initialize the directory from the on-disk contents
     * @param m_onGenerationDrained
     * @throws IOException
     */
    void initializePersistedWindows(Runnable onDrained) throws IOException {
        TreeSet<File> generationDirectories = new TreeSet<File>();
        for (File f : m_exportOverflowDirectory.listFiles()) {
            if (f.isDirectory()) {
                if (!f.canRead() || !f.canWrite() || !f.canExecute()) {
                    throw new RuntimeException("Can't one of read/write/execute directory " + f);
                }
                generationDirectories.add(f);
            }
        }

        for (File generationDirectory : generationDirectories) {
            ExportGeneration generation =
                new ExportGeneration(
                        onDrained,
                        generationDirectory,
                        Long.valueOf(generationDirectory.getName()));
            generation.initializeGenerationFromDisk();
            pushWindow(Long.valueOf(generationDirectory.getName()), generation);
        }
    }

    /**
     * Close all the windows.
     */
    public void closeAllWindows() {
        for (ExportGeneration generation : m_windows.get().values()) {
            generation.close();
        }
    }

    /**
     * How many bytes are queued? Estimate.
     */
    public long estimateQueuedBytes(int partitionId, String signature) {
        TreeMap<Long, ExportGeneration> generations = m_windows.get();
        if (generations.isEmpty()) {
            assert(false);
            return -1L;
        }

        long exportBytes = 0;
        for (ExportGeneration generation : generations.values()) {
            exportBytes += generation.getQueuedExportBytes( partitionId, signature);
        }
        return exportBytes;
    }

    /**
     * Drop data that follows snapshotTxnId
     * @param snapshotTxnId
     */
    public void truncateExportToTxnId(long snapshotTxnId) {
        for (ExportGeneration generation : m_windows.get().values()) {
            generation.truncateExportToTxnId( snapshotTxnId);
        }
    }


    /** Get reference to a specific generation. */
    public ExportGeneration getWindow(long id) {
        return m_windows.get().get(id);
    }

    /** Get reference to first generation */
    public ExportGeneration peekWindow() {
        return m_windows.get().firstEntry().getValue();
    }

    /** Pop and return the oldest generation. */
    public ExportGeneration popWindow() {
        ExportGeneration head = null;
        while (true) {
            TreeMap<Long, ExportGeneration> current = m_windows.get();
            TreeMap<Long, ExportGeneration> copy = new TreeMap<Long, ExportGeneration>(current);
            head = copy.firstEntry().getValue();
            copy.remove(copy.firstEntry().getKey());

            if (m_windows.compareAndSet(current, copy)) {
                break;
            }
        }
        return head;
    }

    /** Push a new generation */
    public void pushWindow(long txnId, ExportGeneration exportGeneration) {
        while(true) {
            TreeMap<Long, ExportGeneration> current = m_windows.get();
            TreeMap<Long, ExportGeneration> copy = new TreeMap<Long, ExportGeneration>(current);
            copy.put(txnId, exportGeneration);

            if (m_windows.compareAndSet(current, copy)) {
                break;
            }
        }
    }


}
