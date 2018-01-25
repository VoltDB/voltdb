/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.exportclient.decode;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.VoltType;

public class TestEntityDecoders extends BaseForDecoderTests {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Test
    public void testJsonEntityDecoder() throws Exception {
        ElasticSearchJsonEntityDecoder.Builder bld = new ElasticSearchJsonEntityDecoder.Builder();
        bld.columnNames(NAMES).columnTypes(TYPES);
        ElasticSearchJsonEntityDecoder dcd = bld.build();

        dcd.add(row);
        dcd.add(row);
        HttpEntity bae = dcd.harvest();
        assertTrue(bae.getContentLength() > 200);
        Scanner sc = new Scanner(bae.getContent());
        final String subJson = "{\"tinyIntField\":10,\"smallIntField\":11,\"integerField\":12,"
                + "\"bigIntField\":13,\"floatField\":14.00014,\"timeStampField\":\""
                + odbcDate
                + "\",\"stringField\":\"sixteen 十六\",\"varBinaryField\":\""
                + base64Yolanda + "\",\"decimalField\":1818.0018,"
                + "\"geogPointField\":\""+ GEOG_POINT.toWKT() + "\",\"geogField\":\"" + GEOG.toWKT() + "\"}";
        String json = sc.useDelimiter("\\A").next();
        assertEquals(2, StringUtils.countMatches(json, subJson));
    }

    @Test
    public void testCSVEntityDecoder() throws Exception {
        CSVEntityDecoder.Builder bld = new CSVEntityDecoder.Builder();
        bld.nullRepresentation("NIL").columnTypes(TYPES).columnNames(NAMES);
        CSVEntityDecoder dcd = bld.build();

        dcd.add(row);
        dcd.add(row);

        HttpEntity bae = dcd.harvest();
        assertTrue(bae.getContentLength() > 200);
    }

    @Test
    public void testAvroEntityDecoder() throws Exception {
        final int intTypeIndex = typeIndex.get(VoltType.INTEGER);
        AvroEntityDecoder.Builder bld = new AvroEntityDecoder.Builder();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(16 * 1024);

        bld.compress(true)
           .packageName("test.me").tableName("Sample")
           .columnTypes(TYPES).columnNames(NAMES);

        AvroEntityDecoder dcd = bld.build();

        dcd.add(row);
        row[intTypeIndex] = 256; dcd.add(row);

        HttpEntity bae = dcd.harvest();
        assertTrue(bae.getContentLength() > 120);

        dcd.getHeaderEntity().writeTo(baos);
        bae.writeTo(baos);

        GenericDatumReader<GenericRecord> areader = new GenericDatumReader<>(dcd.getSchema());
        DataFileReader<GenericRecord> fileReader =
                new DataFileReader<>(new SeekableByteArrayInput(baos.toByteArray()), areader);

        assertEquals("snappy", fileReader.getMetaString("avro.codec"));

        GenericRecord gr;
        Iterator<GenericRecord> itr = fileReader.iterator();

        gr = itr.next();
        assertEquals(gr.get("integerField"), 12);

        gr = itr.next();
        assertEquals(gr.get("integerField"), 256);

        assertFalse(itr.hasNext());
        fileReader.close();

        row[intTypeIndex] = 257; dcd.add(row);
        row[intTypeIndex] = 258; dcd.add(row);

        bae = dcd.harvest();
        assertTrue(bae.getContentLength() > 120 && bae.getContentLength() < 240);
        bae.writeTo(baos);

        fileReader = new DataFileReader<>(new SeekableByteArrayInput(baos.toByteArray()), areader);
        itr = fileReader.iterator();

        gr = itr.next();
        assertEquals(gr.get("integerField"), 12);

        gr = itr.next();
        assertEquals(gr.get("integerField"), 256);

        gr = itr.next();
        assertEquals(gr.get("integerField"), 257);

        gr = itr.next();
        assertEquals(gr.get("integerField"), 258);

        assertFalse(itr.hasNext());
        fileReader.close();

        baos.reset();
        dcd.getHeaderEntity().writeTo(baos);

        row[intTypeIndex] = 259; dcd.add(row);
        row[intTypeIndex] = 260; dcd.add(row);

        bae = dcd.harvest();
        bae.writeTo(baos);

        fileReader = new DataFileReader<>(new SeekableByteArrayInput(baos.toByteArray()), areader);
        itr = fileReader.iterator();

        gr = itr.next();
        assertEquals(gr.get("integerField"), 259);

        gr = itr.next();
        assertEquals(gr.get("integerField"), 260);

        assertFalse(itr.hasNext());
        fileReader.close();
    }
}
