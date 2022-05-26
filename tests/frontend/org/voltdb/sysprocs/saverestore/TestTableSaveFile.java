/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.sysprocs.saverestore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.voltcore.TransactionIdManager;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.DeprecatedDefaultSnapshotDataTarget;
import org.voltdb.NativeSnapshotDataTarget;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.messaging.FastSerializer;

import com.google_voltpatches.common.util.concurrent.Callables;

/**
 * This test also provides pretty good coverage of DefaultSnapshotTarget
 *
 */
public class TestTableSaveFile {
    private static int[] VERSION0 = { 0, 0, 0, 0 };
    private static int[] VERSION1 = { 0, 0, 0, 1 };
    private static int[] VERSION2 = { 0, 0, 0, 2 };
    private static int HOST_ID = 3;
    private static long TXN_ID = TransactionIdManager.makeIdFromComponents(24, 32, 96);
    private static String CLUSTER_NAME = "TEST_CLUSTER";
    private static String DATABASE_NAME = "TEST_DATABASE";
    private static String TABLE_NAME = "TEST_TABLE";
    private static int TOTAL_PARTITIONS = 13;
    private static long TIMESTAMP = System.currentTimeMillis();

    static {
        org.voltdb.NativeLibraryLoader.loadVoltDB();
    }

    private TableSaveFile savefile;

    protected static File s_tmpDirectory = new File(System.getProperty("java.io.tmpdir"));

    @Rule
    public final TemporaryFolder m_folder = new TemporaryFolder(s_tmpDirectory);

    @After
    public void tearDown() throws Exception {
        if (savefile != null) {
            savefile.close();
            savefile = null;
        }
        System.gc();
        System.runFinalization();
    }

    private void serializeChunk(VoltTable chunk,
            NativeSnapshotDataTarget target,
            int partitionId) throws Exception {
        try (FastSerializer fs = new FastSerializer()) {
            fs.writeTable(chunk);
            BBContainer c = fs.getBBContainer();
            ByteBuffer b = c.b();
            b.getInt();
            int headerLength = b.getInt();
            b.position(b.position() + headerLength);// at row count
            BBContainer container = DBBPool.allocateDirect(b.remaining() + 4);
            ByteBuffer payload = container.b();
            payload.putInt(partitionId);
            payload.put(b);
            payload.flip();
            c.discard();

            target.write(Callables.returning(container), -1);
        }
    }

    private Pair<VoltTable, File> generateTestTable(int numberOfItems)
            throws Exception {
        VoltTable.ColumnInfo columnInfo[] = new VoltTable.ColumnInfo[] {
                new ColumnInfo("RT_ID", VoltType.INTEGER),
                new ColumnInfo("RT_NAME", VoltType.STRING),
                new ColumnInfo("RT_INTVAL", VoltType.INTEGER),
                new ColumnInfo("RT_FLOATVAL", VoltType.FLOAT) };
        VoltTable table = new VoltTable(columnInfo);
        final File f = m_folder.newFile();
        ArrayList<Integer> partIds = new ArrayList<Integer>();
        partIds.add(0);
        partIds.add(1);
        partIds.add(2);
        partIds.add(3);
        partIds.add(4);
        NativeSnapshotDataTarget target = createTarget(f, SnapshotPathType.SNAP_PATH.toString(), false, false, partIds, PrivateVoltTableFactory.getSchemaBytes(table),
                VERSION2);

        VoltTable currentChunkTable = new VoltTable(columnInfo,
                columnInfo.length);
        int partitionId = 0;
        for (int i = 0; i < numberOfItems; i++) {
            if (i % 1000 == 0 && i > 0) {
                serializeChunk(currentChunkTable, target, partitionId++);
                currentChunkTable = new VoltTable(columnInfo, columnInfo.length);
            }
            Object[] row = new Object[] { i, "name_" + i, i, new Double(i) };
            currentChunkTable.addRow(row);
            table.addRow(row);
        }
        serializeChunk(currentChunkTable, target, partitionId++);
        target.close();

        return new Pair<VoltTable, File>(table, f, false);
    }

    @Test
    public void testHeaderAccessors() throws Exception {
        System.out.println("Running testHeaderAccessors");
        final File f = m_folder.newFile();
        VoltTable.ColumnInfo columns[] = { new VoltTable.ColumnInfo("Foo", VoltType.STRING) };
        byte[] schema = PrivateVoltTableFactory.getSchemaBytes(new VoltTable(columns));
        ArrayList<Integer> partIds = new ArrayList<Integer>();
        partIds.add(0);
        partIds.add(1);
        partIds.add(2);
        partIds.add(3);
        partIds.add(4);
        NativeSnapshotDataTarget target = createTarget(f, SnapshotPathType.SNAP_PATH.toString(), false, false, partIds, schema, VERSION1);
        target.close();

        FileInputStream fis = new FileInputStream(f);
        savefile = new TableSaveFile(fis, 3, null);

        /*
         * Table header should be 4 bytes metadata length 2 bytes column count 1
         * byte column type 4 byte column name length 3 byte column name
         */
        // Subtract off the fake table column header and the header and
        // tuple lengths
        ByteBuffer tableHeader = savefile.getTableHeader();
        tableHeader.position(0);
        assertEquals(4 + 1 + 2 + 1 + 4 + 3, tableHeader.remaining());
        assertEquals(tableHeader.getInt(), 11);
        assertEquals(tableHeader.get(), Byte.MIN_VALUE);
        assertEquals(tableHeader.getShort(), 1);
        assertEquals(tableHeader.get(), VoltType.STRING.getValue());
        assertEquals(tableHeader.getInt(), 3);
        byte columnNameBytes[] = new byte[3];
        tableHeader.get(columnNameBytes);
        String columnName = new String(columnNameBytes, "UTF-8");
        assertEquals("Foo", columnName);

        for (int i = 0; i < 4; i++) {
            assertEquals(VERSION1[i], savefile.getVersionNumber()[i]);
        }
        assertEquals(TXN_ID, savefile.getTxnId());
        assertEquals(HOST_ID, savefile.getHostId());
        assertEquals(CLUSTER_NAME, savefile.getClusterName());
        assertEquals(DATABASE_NAME, savefile.getDatabaseName());
        assertEquals(TABLE_NAME, savefile.getTableName());
        assertEquals(TIMESTAMP, savefile.getTimestamp());
        assertFalse(savefile.isReplicated());
        int partitionIds[] = savefile.getPartitionIds();
        for (int ii = 0; ii < 5; ii++) {
            assertEquals(ii, partitionIds[ii]);
        }
        assertEquals(TOTAL_PARTITIONS, savefile.getTotalPartitions());
    }

    @Test
    public void testLoadingVersion0Header() throws Exception {
        System.out.println("Running testLoadingVersion0Header");
        final File f = File.createTempFile("foo", "bar");
        f.deleteOnExit();
        VoltTable.ColumnInfo columns[] = new VoltTable.ColumnInfo[] { new VoltTable.ColumnInfo(
                "Foo", VoltType.STRING) };
        VoltTable vt = new VoltTable(columns, 1);
        DeprecatedDefaultSnapshotDataTarget dsdt = new DeprecatedDefaultSnapshotDataTarget(f,
                HOST_ID, CLUSTER_NAME, DATABASE_NAME, TABLE_NAME,
                TOTAL_PARTITIONS, false, new int[] { 0, 1, 2, 3, 4 }, vt,
                TXN_ID, VERSION0);
        dsdt.close();

        FileInputStream fis = new FileInputStream(f);
        savefile = new TableSaveFile(fis, 3, null);

        /*
         * Table header should be 4 bytes metadata length 2 bytes column count 1
         * byte column type 4 byte column name length 3 byte column name
         */
        // Subtract off the fake table column header and the header and
        // tuple lengths
        ByteBuffer tableHeader = savefile.getTableHeader();
        tableHeader.position(0);
        assertEquals(4 + 1 + 2 + 1 + 4 + 3, tableHeader.remaining());
        assertEquals(tableHeader.getInt(), 11);
        assertEquals(tableHeader.get(), Byte.MIN_VALUE);
        assertEquals(tableHeader.getShort(), 1);
        assertEquals(tableHeader.get(), VoltType.STRING.getValue());
        assertEquals(tableHeader.getInt(), 3);
        byte columnNameBytes[] = new byte[3];
        tableHeader.get(columnNameBytes);
        String columnName = new String(columnNameBytes, "UTF-8");
        assertEquals("Foo", columnName);

        for (int i = 0; i < 4; i++) {
            assertEquals(VERSION0[i], savefile.getVersionNumber()[i]);
        }
        assertEquals(TXN_ID, savefile.getTxnId());
        assertEquals(HOST_ID, savefile.getHostId());
        assertEquals(CLUSTER_NAME, savefile.getClusterName());
        assertEquals(DATABASE_NAME, savefile.getDatabaseName());
        assertEquals(TABLE_NAME, savefile.getTableName());
        assertEquals(TransactionIdManager.getTimestampFromTransactionId(TXN_ID), savefile.getTimestamp());
        assertFalse(savefile.isReplicated());
        int partitionIds[] = savefile.getPartitionIds();
        for (int ii = 0; ii < 5; ii++) {
            assertEquals(ii, partitionIds[ii]);
        }
        assertEquals(TOTAL_PARTITIONS, savefile.getTotalPartitions());
    }

    @Test
    public void testFullTable() throws Exception {
        System.out.println("Running testFullTable");
        Pair<VoltTable, File> generated = generateTestTable(1000);
        VoltTable table = generated.getFirst();
        File f = generated.getSecond();

        FileInputStream fis = new FileInputStream(f);
        savefile = new TableSaveFile(fis, 3, null);

        BBContainer c = savefile.getNextChunk();
        try {
            VoltTable test_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b(), false);
            test_table.toString();
            assertEquals(table, test_table);
        } finally {
            c.discard();
        }
    }

    @Test
    public void testChunkTable() throws Exception {
        System.out.println("Running testChunkTable");
        Pair<VoltTable, File> generated = generateTestTable(100000);
        VoltTable table = generated.getFirst();
        File f = generated.getSecond();

        FileInputStream fis = new FileInputStream(f);
        TableSaveFile savefile = new TableSaveFile(fis, 3, null);
        try {
            int expectedPartitionId = 0;
            VoltTable test_table = null;
            VoltTable reaggregate_table = null;
            while (savefile.hasMoreChunks()) {
                final BBContainer c = savefile.getNextChunk();
                if (c == null) {
                    break;
                }
                TableSaveFile.Container cont = (TableSaveFile.Container)c;
                assertEquals(expectedPartitionId, cont.partitionId);
                expectedPartitionId++;
                try {
                    test_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b(), false);
                    if (reaggregate_table == null) {
                        reaggregate_table = test_table.clone(10000);
                    }
                    while (test_table.advanceRow()) {
                        // this will add the active row from test_table
                        reaggregate_table.add(test_table);
                    }
                } finally {
                    c.discard();
                }
            }
            assertEquals(table, reaggregate_table);
        } finally {
            savefile.close();
        }
    }

    protected NativeSnapshotDataTarget createTarget(File file, String pathType, boolean isTerminus, boolean isReplicated, List<Integer> partitionIds,
            byte[] schemaBytes, int[] version) throws IOException {
        return createTarget(file, pathType, isTerminus, HOST_ID, CLUSTER_NAME, DATABASE_NAME, TABLE_NAME, TOTAL_PARTITIONS, isReplicated,
                partitionIds, schemaBytes, TXN_ID, TIMESTAMP, version);
    }

    protected NativeSnapshotDataTarget createTarget(File file, String pathType, boolean isTerminus, int hostId, String clusterName, String databaseName,
            String tableName, int numPartitions, boolean isReplicated, List<Integer> partitionIds, byte[] schemaBytes,
            long txnId, long timestamp, int[] version) throws IOException {
        return new DefaultSnapshotDataTarget(file, hostId, clusterName, databaseName, tableName, numPartitions,
                isReplicated, partitionIds, schemaBytes, txnId, timestamp, version, false, UnaryOperator.identity());
    }
}
