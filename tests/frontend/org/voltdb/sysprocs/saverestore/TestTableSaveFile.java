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

package org.voltdb.sysprocs.saverestore;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import junit.framework.TestCase;

import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.DeprecatedDefaultSnapshotDataTarget;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.Pair;
import org.voltdb.utils.DBBPool.BBContainer;

/**
 * This test also provides pretty good coverage of DefaultSnapshotTarget
 *
 */
public class TestTableSaveFile extends TestCase {
    private static int[] VERSION0 = { 0, 0, 0, 0 };
    private static int[] VERSION1 = { 0, 0, 0, 1 };
    private static int HOST_ID = 3;
    private static long TXN_ID = org.voltdb.TransactionIdManager.makeIdFromComponents(24, 32, 96);
    private static String CLUSTER_NAME = "TEST_CLUSTER";
    private static String DATABASE_NAME = "TEST_DATABASE";
    private static String TABLE_NAME = "TEST_TABLE";
    private static int TOTAL_PARTITIONS = 13;

    static {
        org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
    }

    private void serializeChunk(VoltTable chunk,
            DefaultSnapshotDataTarget target) throws Exception {
        FastSerializer fs = new FastSerializer();

        chunk.writeExternal(fs);
        BBContainer c = fs.getBBContainer();
        ByteBuffer b = c.b;
        b.getInt();
        int headerLength = b.getInt();
        b.position(b.position() + headerLength);// at row count
        final int rowCount = b.getInt();
        ByteBuffer chunkBuffer = ByteBuffer.allocate(b.remaining() + 16
                + target.getHeaderSize());
        chunkBuffer.position(target.getHeaderSize());
        chunkBuffer.mark();
        final CRC32 partitionIdCRC = new CRC32();
        chunkBuffer.putInt(2);
        chunkBuffer.reset();
        byte partitionIdBytes[] = new byte[4];
        chunkBuffer.get(partitionIdBytes);
        partitionIdCRC.update(partitionIdBytes);
        chunkBuffer.putInt((int) partitionIdCRC.getValue());
        final int crcPosition = chunkBuffer.position();
        final CRC32 crc = new CRC32();
        chunkBuffer.position(chunkBuffer.position() + 4);
        chunkBuffer.put(b);
        chunkBuffer.putInt(rowCount);
        chunkBuffer.position(crcPosition + 4);
        crc.update(chunkBuffer.array(), chunkBuffer.position(), chunkBuffer
                .remaining());
        chunkBuffer.position(crcPosition);
        chunkBuffer.putInt((int) crc.getValue());
        chunkBuffer.position(0);
        target.write(new BBContainer(chunkBuffer, 0) {

            @Override
            public void discard() {
                // TODO Auto-generated method stub

            }

        });
    }

    private Pair<VoltTable, File> generateTestTable(int numberOfItems)
            throws Exception {
        VoltTable.ColumnInfo columnInfo[] = new VoltTable.ColumnInfo[] {
                new ColumnInfo("RT_ID", VoltType.INTEGER),
                new ColumnInfo("RT_NAME", VoltType.STRING),
                new ColumnInfo("RT_INTVAL", VoltType.INTEGER),
                new ColumnInfo("RT_FLOATVAL", VoltType.FLOAT) };
        VoltTable table = new VoltTable(columnInfo, columnInfo.length);
        final File f = File.createTempFile("foo", "bar");
        f.deleteOnExit();
        DefaultSnapshotDataTarget dsdt = new DefaultSnapshotDataTarget(f,
                HOST_ID, CLUSTER_NAME, DATABASE_NAME, TABLE_NAME,
                TOTAL_PARTITIONS, false, new int[] { 0, 1, 2, 3, 4 }, table,
                TXN_ID, VERSION1);

        VoltTable currentChunkTable = new VoltTable(columnInfo,
                columnInfo.length);
        for (int i = 0; i < numberOfItems; i++) {
            if (i % 1000 == 0 && i > 0) {
                serializeChunk(currentChunkTable, dsdt);
                currentChunkTable = new VoltTable(columnInfo, columnInfo.length);
            }
            Object[] row = new Object[] { i, "name_" + i, i, new Double(i) };
            currentChunkTable.addRow(row);
            table.addRow(row);
        }
        serializeChunk(currentChunkTable, dsdt);
        dsdt.close();

        return new Pair<VoltTable, File>(table, f, false);
    }

    public void testHeaderAccessors() throws Exception {
        final File f = File.createTempFile("foo", "bar");
        f.deleteOnExit();
        VoltTable.ColumnInfo columns[] = new VoltTable.ColumnInfo[] { new VoltTable.ColumnInfo(
                "Foo", VoltType.STRING) };
        VoltTable vt = new VoltTable(columns, 1);
        DefaultSnapshotDataTarget dsdt = new DefaultSnapshotDataTarget(f,
                HOST_ID, CLUSTER_NAME, DATABASE_NAME, TABLE_NAME,
                TOTAL_PARTITIONS, false, new int[] { 0, 1, 2, 3, 4 }, vt,
                TXN_ID, VERSION1);
        dsdt.close();

        FileInputStream fis = new FileInputStream(f);
        TableSaveFile savefile = new TableSaveFile(fis.getChannel(), 3, null);

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
        assertFalse(savefile.isReplicated());
        int partitionIds[] = savefile.getPartitionIds();
        for (int ii = 0; ii < 5; ii++) {
            assertEquals(ii, partitionIds[ii]);
        }
        assertEquals(TOTAL_PARTITIONS, savefile.getTotalPartitions());
    }

    public void testLoadingVersion0Header() throws Exception {
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
        TableSaveFile savefile = new TableSaveFile(fis.getChannel(), 3, null);

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
        assertFalse(savefile.isReplicated());
        int partitionIds[] = savefile.getPartitionIds();
        for (int ii = 0; ii < 5; ii++) {
            assertEquals(ii, partitionIds[ii]);
        }
        assertEquals(TOTAL_PARTITIONS, savefile.getTotalPartitions());
    }

    public void testFullTable() throws Exception {
        Pair<VoltTable, File> generated = generateTestTable(1000);
        VoltTable table = generated.getFirst();
        File f = generated.getSecond();

        FileInputStream fis = new FileInputStream(f);
        TableSaveFile savefile = new TableSaveFile(fis.getChannel(), 3, null);

        BBContainer c = savefile.getNextChunk();
        try {
            VoltTable test_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b, false);
            test_table.toString();
            assertEquals(table, test_table);
        } finally {
            c.discard();
        }
    }

    public void testChunkTable() throws Exception {
        Pair<VoltTable, File> generated = generateTestTable(100000);
        VoltTable table = generated.getFirst();
        File f = generated.getSecond();

        FileInputStream fis = new FileInputStream(f);
        TableSaveFile savefile = new TableSaveFile(fis.getChannel(), 3, null);

        VoltTable test_table = null;
        VoltTable reaggregate_table = null;
        while (savefile.hasMoreChunks()) {
            BBContainer c = savefile.getNextChunk();
            try {
                test_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b, false);
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
    }
}
