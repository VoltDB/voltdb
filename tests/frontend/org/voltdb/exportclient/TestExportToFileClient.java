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

package org.voltdb.exportclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.voltdb.MockVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.ExportDecoderBase.RestartBlockException;

import com.google_voltpatches.common.base.Charsets;
import org.apache.commons.io.FileUtils;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ExportToFileClient.class)
public class TestExportToFileClient extends ExportClientTestBase {

    static final String m_dir = "/tmp" + File.separator + System.getProperty("user.name");
    private static MockVoltDB s_mockVoltDB = new MockVoltDB("foo", "bar");

    @Override
    @Before
    public void setup()
    {
        super.setup();
        try {
            FileUtils.deleteDirectory(new File(m_dir));
            (new File(m_dir)).mkdirs();
        } catch (IOException e) {
            fail(e.getMessage());
        }
        ExportToFileClient.TEST_VOLTDB_ROOT = m_dir;
        VoltDB.replaceVoltDBInstanceForTest(s_mockVoltDB);
    }

    @Test
    public void testExportToFileClientConfiguration() throws Exception {
        ExportToFileClient eclient = new ExportToFileClient();
        Properties props = new Properties();

        // Missing nonce
        try {
            eclient.configure(props);
            fail("Missing nonce");
        } catch (IllegalArgumentException ex) {}

        // Invalid type
        props.put("nonce", "mynonce");
        props.put("type", "zsv");
        try {
            eclient.configure(props);
            fail("Invalid type");
        } catch (IllegalArgumentException ex) {}

        // Bare minimum, should succeed
        props.put("type", "csv");
        eclient.configure(props);

        // Invalid period
        props.put("output", "/tmp/");
        props.put("period", "0");
        try {
            eclient.configure(props);
            fail("Invalid period");
        } catch (IllegalArgumentException ex) {}

        // Valid period
        eclient = new ExportToFileClient();
        props.put("output", "/tmp/");
        props.put("period", "1");
        eclient.configure(props);

        // Invalid binary encoding
        eclient = new ExportToFileClient();
        props.put("binaryencoding", "blah");
        try {
            eclient.configure(props);
            fail("Invalid binary encoding");
        } catch (IllegalArgumentException e) {}

        // Valid binary encoding
        props.put("binaryencoding", "base64");
        eclient.configure(props);

        // Invalid directory - can't create
        eclient = new ExportToFileClient();
        props.put("outdir", "/root/thereisnosuchdirectory");
        try {
            eclient.configure(props);
            fail("Invalid directory");
        } catch (IllegalArgumentException e) {}

        // Invalid directory - can't write
        eclient = new ExportToFileClient();
        props.put("outdir", "/root");
        try {
            eclient.configure(props);
            fail("Invalid directory");
        } catch (IllegalArgumentException e) {}

        // Relative directory
        eclient = new ExportToFileClient();
        props.put("outdir", "my_exports");
        try {
            //This should pass.
            eclient.configure(props);
        } catch (IllegalArgumentException e) {}

        // Not enough delimiters
        eclient = new ExportToFileClient();
        props.put("delimiters", ",,,");
        try {
            eclient.configure(props);
            fail("Invalid delimiters");
        } catch (IllegalArgumentException e) {}

        // Too many delimiters
        eclient = new ExportToFileClient();
        props.put("delimiters", ",,,,,");
        try {
            eclient.configure(props);
            fail("Invalid delimiters");
        } catch (IllegalArgumentException e) {}

        // Contains special characters
        eclient = new ExportToFileClient();
        props.put("delimiters", "\1*,\n");
        eclient.configure(props);
    }

    @Test
    public void testPeriodUnits() throws Exception
    {
        unitTest("123s", 123);
        unitTest("123m", 123 * 60);
        unitTest("123h", 123 * 60 * 60);
        unitTest("123d", 123 * 60 * 60 * 24);
        unitTest("123",  123 * 60); // default to minutes
        unitTest("123x", -1); // fail
    }

    private void unitTest(String value, int expected) {
        ExportToFileClient client = new ExportToFileClient();
        String nonce = "TEST_" + value;
        try {
            Properties props = new Properties();
            props.put("nonce", nonce);
            props.put("period", value);
            client.configure(props);
        }
        catch (Exception ex) {
            if (expected < 0) {
                System.out.printf("%s failed as expected: %s\n", nonce, ex.getMessage());
            } else {
                fail(String.format("%s threw unexpected exception: %s", nonce, ex));
            }
            return;
        }
        if (expected < 0) {
            fail(String.format("%s was expected to fail but resulted in %s", nonce, client.m_periodSecs));
        }
        else {
            assertEquals(String.format("%s gave unexpected result %s", nonce, client.m_periodSecs),
                         expected, client.m_periodSecs);
        }
    }

    @Test
    public void testFileRollingUnbatched() throws Exception
    {
        final long startTs = System.currentTimeMillis();
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", Long.toString(System.currentTimeMillis()));
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("period", "1s"); // 1 second rolling period
        client.configure(props);

        final AdvertisedDataSource source = constructTestSource(false, 0);
        final ExportToFileClient.ExportToFileDecoder decoder = client.constructExportDecoder(source);

        long l = System.currentTimeMillis();
        vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                /* partitioning column */ (short) 2,
                3, 4, 5.5, 6, "xx", new BigDecimal(88),
                GEOG_POINT, GEOG);
        vtable.advanceRow();
        byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", 0, 1L);
        ByteBuffer bb = ByteBuffer.wrap(rowBytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        int schemaSize = bb.getInt();
        ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
        bb.getInt(); // row size
        ExportRow row = ExportRow.decodeRow(schemaRow, 0, bb);
        decoder.onBlockStart(row);
        decoder.processRow(row);
        decoder.onBlockCompletion(row);

        // The file should rollover after 1s
        while (System.currentTimeMillis() - startTs < 60 * 1000) { // timeout after 1 minute
            final File dir = new File(m_dir);
            final File[] files = dir.listFiles();
            int index;
            if (files != null && files.length > 0 && (index = findFileNotStartWithActive(files)) >= 0) {
                assertTrue(System.currentTimeMillis() - startTs > 1000);
                verifyContent(files[index], l);
                return;
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for file to roll over");
    }

    // Find the index of file array whose name does not start with "active"; -1 when not found.
    private static int findFileNotStartWithActive(File[] files) {
        if (files == null || files.length == 0) {
            return -1;
        } else {
            for (int indx = 0; indx < files.length; ++indx) {
                if (!files[indx].getName().startsWith("active")) {
                    return indx;
                }
            }
            return -1;
        }
    }

    @Test
    public void testFileRollingBatched() throws Exception
    {
        final long startTs = System.currentTimeMillis();
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", Long.toString(System.currentTimeMillis()));
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("period", "1s"); // 1 second rolling period
        props.put("batched", "true");
        client.configure(props);

        final AdvertisedDataSource source = constructTestSource(false, 0);
        final ExportToFileClient.ExportToFileDecoder decoder = client.constructExportDecoder(source);

        long l = System.currentTimeMillis();
        vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                /* partitioning column */ (short) 2,
                3, 4, 5.5, 6, "xx", new BigDecimal(88),
                GEOG_POINT, GEOG);
        vtable.advanceRow();
        byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", 0, 1L);
        ByteBuffer bb = ByteBuffer.wrap(rowBytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        int schemaSize = bb.getInt();
        ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
        bb.getInt(); // row size
        ExportRow row = ExportRow.decodeRow(schemaRow, 0, bb);
        decoder.onBlockStart(row);
        decoder.processRow(row);
        decoder.onBlockCompletion(row);

        // The file should rollover after 1s
        File rolledOver = null;
        while (System.currentTimeMillis() - startTs < 60 * 1000) { // timeout after 1 minute
            final File dir = new File(m_dir);
            final File[] subdirs = dir.listFiles();
            if (subdirs != null) {
                // Locate the rolled over dir
                for (File file : subdirs) {
                    if (!file.getName().startsWith("active")) {
                        rolledOver = file;
                        break;
                    }
                }
                if (rolledOver != null) {
                    // batched mode uses directory
                    assertTrue(rolledOver.isDirectory());
                    assertTrue(System.currentTimeMillis() - startTs > 1000);

                    final File[] files = rolledOver.listFiles();
                    assertEquals(1, files.length);
                    verifyContent(files[0], l);
                    break;
                }
            }
            Thread.sleep(100);
        }
        assertNotNull("Timed out waiting for file to roll over", rolledOver);
    }

    @Test
    public void testExportFileUnwritable() throws Exception
    {
        final long startTs = System.currentTimeMillis();
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", Long.toString(System.currentTimeMillis()));
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("period", "100s"); // 100 second rolling period
        props.put("with-schema", "false"); // disable write JSON representation
        client.configure(props);

        CSVWriter csvWriter =  Mockito.mock(CSVWriter.class);
        PowerMockito.whenNew(CSVWriter.class).withAnyArguments().thenReturn(csvWriter);

        // return error for first two times (mock invoke CSVWriter.checkError return error case)
        Mockito.doReturn(true).
            doReturn(true).
            doReturn(false).
            when(csvWriter).checkError();


        final AdvertisedDataSource source = constructTestSource(false, 0);
        final ExportToFileClient.ExportToFileDecoder decoder = client.constructExportDecoder(source);

        int retry = 0;
        long l;
        while (true) {
            try {
                l = System.currentTimeMillis();
                vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                /* partitioning column */(short) 2, 3, 4, 5.5, 6, "xx", new BigDecimal(88),
                GEOG_POINT, GEOG);
                vtable.advanceRow();
                byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", 0, 1L);
                ByteBuffer bb = ByteBuffer.wrap(rowBytes);
                bb.order(ByteOrder.LITTLE_ENDIAN);
                int schemaSize = bb.getInt();
                ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
                bb.getInt(); // row size
                ExportRow row = ExportRow.decodeRow(schemaRow, 0, bb);
                decoder.onBlockStart(row);
                decoder.processRow(row);
                decoder.onBlockCompletion(row);
                break;
            } catch (RestartBlockException e) {
                e.printStackTrace();
                retry ++ ;
            }
        }

        assertEquals(2, retry);
    }

    @Test
    public void testSchemaFileUnwritable() throws Exception
    {
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", Long.toString(System.currentTimeMillis()));
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("period", "1s"); // 1 second rolling period
        props.put("with-schema", "true"); // enable write JSON representation
        client.configure(props);

        FileOutputStream fos = Mockito.mock(FileOutputStream.class);
        PowerMockito.whenNew(FileOutputStream.class).withAnyArguments().thenReturn(fos);

        // throw exception first two times on FileOutputStream flush (mock cannot written schema file case)
        Mockito.doThrow(new IOException("Not Enough Space.")).
            doThrow(new IOException("Not Enough Space.")).
            doCallRealMethod().
            when(fos).flush();

        final AdvertisedDataSource source = constructTestSource(false, 0);
        final ExportToFileClient.ExportToFileDecoder decoder = client.constructExportDecoder(source);

        int retry = 0;
        long l;
        while (true) {
            try {
                l = System.currentTimeMillis();
                vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                        /* partitioning column */(short) 2, 3, 4, 5.5, 6, "xx", new BigDecimal(88),
                        GEOG_POINT, GEOG);
                vtable.advanceRow();
                byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", 0, 1L);
                ByteBuffer bb = ByteBuffer.wrap(rowBytes);
                bb.order(ByteOrder.LITTLE_ENDIAN);
                int schemaSize = bb.getInt();
                ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
                bb.getInt(); // row size
                ExportRow row = ExportRow.decodeRow(schemaRow, 0, bb);
                decoder.onBlockStart(row);
                decoder.processRow(row);
                decoder.onBlockCompletion(row);
                break;
            } catch (RestartBlockException e) {
                e.printStackTrace();
                retry ++ ;
            }
        }
        assertEquals(2, retry);
    }

    @Test
    public void testFailGetWriter() throws Exception
    {
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", Long.toString(System.currentTimeMillis()));
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("period", "1s"); // 1 second rolling period
        props.put("with-schema", "true"); // enable write JSON representation
        client.configure(props);

        FileOutputStream fos = Mockito.mock(FileOutputStream.class);
        FileNotFoundException fnfe = new FileNotFoundException("The file exists but is a directory rather than a regular file, "
                + "does not exist but cannot be created, or cannot be opened for any other reason");

        // throw IOexception first two times on create new FileOutputStream (mock cannot create csv file case)
        PowerMockito.whenNew(FileOutputStream.class).withParameterTypes(File.class, boolean.class).withArguments(Matchers.any(), Matchers.anyBoolean()).
            thenThrow(fnfe).
            thenThrow(fnfe).
            thenReturn(fos);


        final AdvertisedDataSource source = constructTestSource(false, 0);
        final ExportToFileClient.ExportToFileDecoder decoder = client.constructExportDecoder(source);

        int retry = 0;
        long l;
        while (true) {
            try {
                l = System.currentTimeMillis();
                vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                        /* partitioning column */(short) 2, 3, 4, 5.5, 6, "xx", new BigDecimal(88),
                        GEOG_POINT, GEOG);
                vtable.advanceRow();
                byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", 0, 1L);
                ByteBuffer bb = ByteBuffer.wrap(rowBytes);
                bb.order(ByteOrder.LITTLE_ENDIAN);
                int schemaSize = bb.getInt();
                ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
                bb.getInt(); // row size
                ExportRow row = ExportRow.decodeRow(schemaRow, 0, bb);
                decoder.onBlockStart(row);
                decoder.processRow(row);
                decoder.onBlockCompletion(row);
                break;
            } catch (RestartBlockException e) {
                e.printStackTrace();
                retry ++ ;
            }
        }

        assertEquals(2, retry);
    }

    @Test
    public void testFilenameBatchedUnique() throws Exception
    {
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", Long.toString(System.currentTimeMillis()));
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("period", "1s"); // 1 second rolling period
        props.put("with-schema", "true");
        props.put("batched", "true");
        props.put("uniquenames", "true");
        client.configure(props);
        final AdvertisedDataSource source = constructTestSource(false, 0);
        final ExportToFileClient.ExportToFileDecoder decoder = client.constructExportDecoder(source);

        long l = System.currentTimeMillis();
        vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                /* partitioning column */ (short) 2,
                3, 4, 5.5, 6, "xx", new BigDecimal(88),
                GEOG_POINT, GEOG);
        vtable.advanceRow();
        byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", 0, 1L);
        ByteBuffer bb = ByteBuffer.wrap(rowBytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        int schemaSize = bb.getInt();
        ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
        bb.getInt(); // row size
        ExportRow row = ExportRow.decodeRow(schemaRow, 0, bb);
        decoder.onBlockStart(row);
        decoder.processRow(row);
        decoder.onBlockCompletion(row);


        String invalidName = null;
        final File dir = new File(m_dir);
        final File[] subdirs = dir.listFiles();

        List<File> allFiles = new ArrayList<>();
        if (subdirs != null) {
            for (File file : subdirs) {
                if (file.isDirectory()) {
                    allFiles.addAll(Arrays.asList(file.listFiles()));
                }
                else {
                    allFiles.add(file);
                }
            }
            for (File file : allFiles) {
                String name = file.getName();
                if (!(name.matches("\\d{19}\\-mytable\\-\\(\\d\\)\\.[a-z]{3}") ||
                      name.matches("\\d{19}\\-mytable\\-\\(\\d\\)-schema\\.json"))) {
                    invalidName = name;
                    break;
                }
            }
        }

        String failMsg = String.format("invalid name: %s", invalidName);
        assertNull(failMsg, invalidName);
    }

    @Test
    public void testFilenameBatchedNotUnique() throws Exception
    {
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", Long.toString(System.currentTimeMillis()));
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("period", "1s"); // 1 second rolling period
        props.put("with-schema", "true");
        props.put("batched", "true");
        props.put("uniquenames", "false");
        client.configure(props);
        final AdvertisedDataSource source = constructTestSource(false, 0);
        final ExportToFileClient.ExportToFileDecoder decoder = client.constructExportDecoder(source);

        long l = System.currentTimeMillis();
        vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                /* partitioning column */ (short) 2,
                3, 4, 5.5, 6, "xx", new BigDecimal(88),
                GEOG_POINT, GEOG);
        vtable.advanceRow();
        byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", 0, 1L);
        ByteBuffer bb = ByteBuffer.wrap(rowBytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        int schemaSize = bb.getInt();
        ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
        bb.getInt(); // row size
        ExportRow row = ExportRow.decodeRow(schemaRow, 0, bb);
        decoder.onBlockStart(row);
        decoder.processRow(row);
        decoder.onBlockCompletion(row);

        String invalidName = null;
        final File dir = new File(m_dir);
        final File[] subdirs = dir.listFiles();

        List<File> allFiles = new ArrayList<>();
        if (subdirs != null) {
            for (File file : subdirs) {
                if (file.isDirectory()) {
                    allFiles.addAll(Arrays.asList(file.listFiles()));
                }
                else {
                    allFiles.add(file);
                }
            }
            for (File file : allFiles) {
                String name = file.getName();
                if (!(name.matches("\\d{19}\\-mytable\\.[a-z]{3}") ||
                      name.matches("\\d{19}\\-mytable-schema\\.json"))) {
                    invalidName = name;
                    break;
                }
            }
        }

        String failMsg = String.format("invalid name: %s", invalidName);
        assertNull(failMsg, invalidName);
    }

    @Test
    public void testFilenameUnbatchedUnique() throws Exception
    {
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", Long.toString(System.currentTimeMillis()));
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("period", "1s"); // 1 second rolling period
        props.put("with-schema", "true");
        props.put("batched", "false");
        props.put("uniquenames", "true");
        client.configure(props);
        final AdvertisedDataSource source = constructTestSource(false, 0);
        final ExportToFileClient.ExportToFileDecoder decoder = client.constructExportDecoder(source);

        long l = System.currentTimeMillis();
        vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                /* partitioning column */ (short) 2,
                3, 4, 5.5, 6, "xx", new BigDecimal(88),
                GEOG_POINT, GEOG);
        vtable.advanceRow();
        byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", 0, 1L);
        ByteBuffer bb = ByteBuffer.wrap(rowBytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        int schemaSize = bb.getInt();
        ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
        bb.getInt(); // row size
        ExportRow row = ExportRow.decodeRow(schemaRow, 0, bb);
        decoder.onBlockStart(row);
        decoder.processRow(row);
        decoder.onBlockCompletion(row);


        boolean validName = true;
        final File dir = new File(m_dir);
        final File[] subdirs = dir.listFiles();

        if (subdirs != null) {
            for (File file : subdirs) {
                if (!(file.getName().matches(".*\\d+\\-\\d\\-mytable\\-\\d+\\-\\(\\d\\)\\.[a-z]{3}") || file.getName().matches(".*\\d+\\-\\d\\-mytable\\-\\d+\\-\\(\\d\\)-schema\\.json"))) {
                    validName = false;
                    break;
                }
            }
        }
        assertTrue(validName);
    }

    @Test
    public void testFilenameUnBatchedNotUnique() throws Exception
    {
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", Long.toString(System.currentTimeMillis()));
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("period", "1s"); // 1 second rolling period
        props.put("with-schema", "true");
        props.put("batched", "false");
        props.put("uniquenames", "false");
        client.configure(props);
        final AdvertisedDataSource source = constructTestSource(false, 0);
        final ExportToFileClient.ExportToFileDecoder decoder = client.constructExportDecoder(source);

        long l = System.currentTimeMillis();
        vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                /* partitioning column */ (short) 2,
                3, 4, 5.5, 6, "xx", new BigDecimal(88),
                GEOG_POINT, GEOG);
        vtable.advanceRow();
        byte[] rowBytes = ExportEncoder.encodeRow(vtable, "mytable", 0, 1L);
        ByteBuffer bb = ByteBuffer.wrap(rowBytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        int schemaSize = bb.getInt();
        ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
        bb.getInt(); // row size
        ExportRow row = ExportRow.decodeRow(schemaRow, 0, bb);
        decoder.onBlockStart(row);
        decoder.processRow(row);
        decoder.onBlockCompletion(row);

        boolean validName = true;
        final File dir = new File(m_dir);
        final File[] subdirs = dir.listFiles();

        if (subdirs != null) {
            for (File file : subdirs) {
                if (!(file.getName().matches(".*\\d+\\-\\d\\-mytable\\-\\d+\\.[a-z]{3}") || file.getName().matches(".*\\d+\\-\\d\\-mytable\\-\\d+-schema\\.json"))) {
                    validName = false;
                    break;
                }
            }
        }
        assertTrue(validName);
    }

    void verifyContent(File f, long ts) throws IOException
    {
        assertEquals(String.format("\"%d\",\"%d\",\"%d\",\"0\",\"%d\",\"%d\",\"1\",\"2\",\"3\",\"4\",\"5.5\",\"1970-01-01 00:00:00.000\",\"xx\",\"88.000000000000\","
                + "\"" + GEOG_POINT.toWKT() + "\",\"" + GEOG.toWKT() + "\"", ts, ts, ts, ts, ts),
                new String(Files.readAllBytes(f.toPath()), Charsets.UTF_8).trim());
    }

    @Test
    public void testRenameStrandedUnbatchedFiles() throws Exception {
        // create some fake csv files
        String[] times = { "20220101000000", "20220101010000", "20220101020000", "20220101030000" };
        String[] tables = { "TABLE", "LISTE" };
        String nonce = "FileExport";
        int gen = 1000;
        for (String time : times) {
            for (String table : tables) {
                String name = "active-" + nonce + "-" + String.format("%019d", gen) + "-" + table + "-" + time + ".csv";
                File file = new File(m_dir, name);
                boolean created = file.createNewFile();
                assertTrue("collision on create", created);
                gen++;
            }
        }

        // check for files
        File outdir = new File(m_dir);
        File[] files = outdir.listFiles();
        int found = 0;
        for (File f : files) {
            String filename = f.getName();
            if (filename.startsWith("active-")) {
                found++;
            }
        }
        assertEquals("Didn't find expected files", 8, found);

        // configure client
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", nonce);
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("batched", "false");
        client.configure(props);

        // check the files were renamed
        int foundActive = 0;
        int foundInactive = 0;
        files = outdir.listFiles();
        for (File f : files) {
            String filename = f.getName();
            if (filename.startsWith("active-")) {
                foundActive++;
            } else {
                foundInactive++;
            }
        }
        assertEquals("stranded active file still there", 0, foundActive);
        assertEquals("stranded active files not renamed", 8, foundInactive);

    }

    @Test
    public void testRenameStrandedBatchedFiles() throws Exception {

        // create some fake csv files
        String[] times = { "20220101000000", "20220101010000", "20220101020000", "20220101030000" };
        String[] tables = { "TABLE", "LISTE" };
        String nonce = "FileExport";
        int gen = 1000;
        for (String time : times) {
            String dirName = "active-" + nonce + "-" + time;
            File dir = new File(m_dir, dirName);
            boolean created = dir.mkdir();
            assertTrue("collision on dir create", created);
            for (String table : tables) {
                String filename = String.format("%019d", gen) + "-" + table + ".csv";
                File file = new File(dir, filename);
                created = file.createNewFile();
                assertTrue("collision on create", created);
                gen++;
            }
        }

        // check for files
        File outdir = new File(m_dir);
        File[] files = outdir.listFiles();
        int found = 0;
        for (File f : files) {
            String filename = f.getName();
            if (filename.startsWith("active-")) {
                found++;
            }
        }
        assertEquals("Didn't find expected stranded batch folders", 4, found);

        // configure client
        ExportToFileClient client = new ExportToFileClient();
        Properties props = new Properties();
        props.put("nonce", nonce);
        props.put("type", "csv");
        props.put("outdir", m_dir);
        props.put("batched", "true");
        client.configure(props);

        // check the files were renamed
        int foundActive = 0;
        int foundInactive = 0;
        files = outdir.listFiles();
        for (File f : files) {
            String filename = f.getName();
            if (filename.startsWith("active-")) {
                foundActive++;
            } else {
                foundInactive++;
            }
        }
        assertEquals("new active folder not found", 1, foundActive);
        assertEquals("Stranded folders not renamed", 4, foundInactive);

    }

}
