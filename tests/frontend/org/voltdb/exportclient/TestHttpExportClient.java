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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.localserver.LocalTestServer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.exportclient.decode.CSVEntityDecoder;
import org.voltdb.exportclient.decode.EndpointExpander;

public class TestHttpExportClient extends ExportClientTestBase {
    static File m_schemaOut;
    LocalTestServer m_server = null;
    VoltTable m_table;
    static final long DEFAULT_PARTITION_ID = 0L;
    static final long OPERATION = 1;
    long m_testStartTime = System.currentTimeMillis();;

    void setupServer() throws Exception
    {
        m_server = new LocalTestServer(null, null);
        m_server.start();
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        m_schemaOut = File.createTempFile("avro-schema.", ".tmp");
        if (!m_schemaOut.delete() || !m_schemaOut.mkdir()) {
            throw new RuntimeException("Cannot create avro schema directory\"" + m_schemaOut + "\"");
        }
        if (   !m_schemaOut.isDirectory()
            || !m_schemaOut.canRead()
            || !m_schemaOut.canWrite()
            || !m_schemaOut.canExecute()) {
            throw new RuntimeException("Cannot access avro schema directory\"" + m_schemaOut + "\"");
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
        FileUtils.deleteDirectory(m_schemaOut);
    }

    @Override
    @Before
    public void setup()
    {
        super.setup();
        m_table = new VoltTable(vtable.getTableSchema());
    }

    @After
    public void tearDown() throws Exception
    {
        m_table.clearRowData();
        if (m_server != null) {
            m_server.stop();
        }
        m_server = null;
    }

    @Test
    public void testEmptyConfigValidation() throws Exception
    {
        final HttpExportClient dut = new HttpExportClient();
        final Properties config = new Properties();
        try {
            dut.configure(config);
            fail("Empty config");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("must provide an endpoint"));
        }
    }

    @Test
    public void testBadlySpecifiedAvroSchemaLocation() throws Exception
    {
        final HttpExportClient dut = new HttpExportClient();
        final Properties config = new Properties();

        // Wrong scheme
        config.setProperty("endpoint", "http://fakehost/%t/%p");
        config.setProperty("avro.schema.location","http://foo/webhdfs/v1");
        try {
            dut.configure(config);
            fail("avro schema location check");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("could not expand"));
        }
        config.setProperty("avro.schema.location","http://foo/watsaname");
        try {
            dut.configure(config);
            fail("avro schema location check");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("is not a webhdfs URL"));
        }
    }

    @Test
    public void testSchemeValidation() throws Exception
    {
        final HttpExportClient dut = new HttpExportClient();
        final Properties config = new Properties();
        URI dutURI = null;

        // Wrong scheme
        config.setProperty("endpoint", "htttp://fakehost");
        try {
            dut.configure(config);
            fail("Wrong scheme");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("could not expand"));
        }

        // Correct schemes
        config.setProperty("endpoint", "http://fakehost/%t/%p");
        dut.configure(config);
        dutURI = new URI(EndpointExpander.expand(dut.m_endpoint,"SAMPLE_T",1,123L,new Date()));
        assertEquals("http", dutURI.getScheme());

        config.setProperty("endpoint", "https://fakehost/%t/%g/%p");
        dut.configure(config);
        dutURI = new URI(EndpointExpander.expand(dut.m_endpoint,"SAMPLE_T",1,123L,new Date()));
        assertEquals("https", dutURI.getScheme());
    }

    @Test
    public void testMethodValidation() throws Exception
    {
        final HttpExportClient dut = new HttpExportClient();
        final Properties config = new Properties();

        // Wrong HTTP method
        config.setProperty("endpoint", "http://fakehost/%p/%t");
        config.setProperty("method", "head");
        try {
            dut.configure(config);
            fail("Wrong HTTP method");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("method may only be"));
        }

        // Correct methods
        config.setProperty("method", "get");
        dut.configure(config);
        assertEquals(HttpExportClient.HttpMethod.GET, dut.m_method);

        config.setProperty("method", "post");
        dut.configure(config);
        assertEquals(HttpExportClient.HttpMethod.POST, dut.m_method);

        config.setProperty("method", "put");
        dut.configure(config);
        assertEquals(HttpExportClient.HttpMethod.PUT, dut.m_method);
    }

    @Test
    public void testBatchValidation() throws Exception
    {
        final HttpExportClient dut = new HttpExportClient();
        final Properties config = new Properties();

        // Bad endpoint
        config.setProperty("endpoint", "http://fakehost/maccarena/v1/root/%p/%t");
        config.setProperty("method", "post");
        config.setProperty("batch.mode","true");
        try {
            dut.configure(config);
            fail("Bad endpoint");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().toLowerCase().contains("batch"));
        }

        // Wrong HTTP method
        config.setProperty("endpoint", "http://fakehost/maccarena/v1/root/%g/%p/%t");
        config.setProperty("method", "get");
        try {
            dut.configure(config);
            fail("Wrong HTTP method");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().toLowerCase().contains("batch"));
        }

        // Correct methods
        config.setProperty("method", "post");
        dut.configure(config);
        assertEquals(HttpExportClient.HttpMethod.POST, dut.m_method);

        config.setProperty("method", "put");
        dut.configure(config);
        assertEquals(HttpExportClient.HttpMethod.PUT, dut.m_method);

        config.clear();

        config.setProperty("endpoint", "http://fakehost/webhdfs/v1/root/%g/%p/%t");
        config.setProperty("method", "put");
        dut.configure(config);
        assertEquals(HttpExportClient.HttpMethod.POST, dut.m_method);

        config.setProperty("method", "post");
        dut.configure(config);
        assertEquals(HttpExportClient.HttpMethod.POST, dut.m_method);

        config.setProperty("method", "get");
        dut.configure(config);
        assertEquals(HttpExportClient.HttpMethod.POST, dut.m_method);
    }

    @Test
    public void testSignatureMethodValidation() throws Exception
    {
        final HttpExportClient dut = new HttpExportClient();
        final Properties config = new Properties();

        // Wrong signature method
        config.setProperty("endpoint", "http://fakehost/%p/%t");
        config.setProperty("signatureMethod", "HmacSHA128");
        try {
            dut.configure(config);
            fail("Wrong signature method");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("signature method"));
        }

        // Correct signature methods
        config.setProperty("signatureMethod", HttpExportClient.HmacSHA1);
        dut.configure(config);
        assertEquals(HttpExportClient.HmacSHA1, dut.m_signatureMethod);

        config.setProperty("signatureMethod", HttpExportClient.HmacSHA256);
        dut.configure(config);
        assertEquals(HttpExportClient.HmacSHA256, dut.m_signatureMethod);
    }

    @Test
    public void testContentTypeValidation() throws Exception
    {
        final HttpExportClient dut = new HttpExportClient();
        final Properties config = new Properties();

        // Wrong content type for signing
        config.setProperty("endpoint", "http://fakehost/%p/%t");
        config.setProperty("type", "csv");
        config.setProperty("secret", "youcantseeme");
        try {
            dut.configure(config);
            fail("Wrong content type for signing");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }

        // Correct config
        config.setProperty("type", "form");
        dut.configure(config);
        assertEquals(ContentType.APPLICATION_FORM_URLENCODED, dut.m_contentType);
    }

    @Test
    public void testNonHdfsDefaultConfig() throws Exception
    {
        final HttpExportClient dut = new HttpExportClient();
        final Properties config = new Properties();
        config.setProperty("endpoint", "http://fakehost/%t/%p");
        dut.configure(config);
        assertEquals("http://fakehost/%t/%p", dut.m_endpoint);
        assertEquals(HttpExportClient.HttpMethod.POST, dut.m_method);
        assertFalse(dut.m_isHdfs);
        assertEquals(ContentType.APPLICATION_FORM_URLENCODED, dut.m_contentType);
        assertNull(dut.m_secret);
        assertEquals("Signature", dut.m_signatureName);
        assertEquals(HttpExportClient.HmacSHA1, dut.m_signatureMethod);
    }

    @Test
    public void testHdfsDefaultConfig() throws Exception
    {
        final HttpExportClient dut = new HttpExportClient();
        final Properties config = new Properties();
        config.setProperty("endpoint", "http://fakehost/webhdfs/v1/root/%g/%p/%t");
        dut.configure(config);
        assertEquals("http://fakehost/webhdfs/v1/root/%g/%p/%t", dut.m_endpoint);
        assertEquals(HttpExportClient.HttpMethod.POST, dut.m_method);
        assertTrue(dut.m_isHdfs);
        assertEquals(CSVEntityDecoder.CSVContentType, dut.m_contentType);
        assertNull(dut.m_secret);
        assertEquals("Signature", dut.m_signatureName);
        assertEquals(HttpExportClient.HmacSHA1, dut.m_signatureMethod);
    }

    @Test
    public void testFormNoSigningPrepareBody() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();
        config.setProperty("type", "form");

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                final String body = EntityUtils.toString(((HttpEntityEnclosingRequest) httpRequest).getEntity());
                if (body.equals("Tinyint=1&Smallint=2&Integer=3&Bigint=4&Float=5.5&Timestamp=1970-01-01T00%3A00%3A00.000%2B0000&String=x%20x&Decimal=88.000000000000&"
                        + "GeogPoint=POINT%20%28-122.0%2037.0%29&Geog=POLYGON%20%28%280.0%200.0%2C%201.0%201.0%2C%200.0%201.0%2C%200.0%200.0%29%29")) {
                    success.set(true);
                } else {
                    System.err.println(body);
                }
            }
        });

        assertTrue(success.get());
    }

    @Test
    public void testFormNoSigningPrepareBodyWithNullValues() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();
        config.setProperty("type", "form");

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                final String body = EntityUtils.toString(((HttpEntityEnclosingRequest) httpRequest).getEntity());
                if (body.equals("Tinyint=%00&Smallint=2&Integer=3&Bigint=4&Float=%00&Timestamp=%00"
                        + "&String=%00&Decimal=88.000000000000&GeogPoint=%00&Geog=%00")) {
                    success.set(true);
                } else {
                    System.err.println(body);
                }
            }
        }, m_testStartTime, true);

        assertTrue(success.get());
    }

    @Test
    public void testFormSigningPrepareBody() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();
        config.setProperty("type", "form");
        config.setProperty("signatureMethod", HttpExportClient.HmacSHA256);
        config.setProperty("signatureName", "Signature");
        config.setProperty("secret", "mysecretsecretkey");

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                final String body = EntityUtils.toString(((HttpEntityEnclosingRequest) httpRequest).getEntity());
                if (body.equalsIgnoreCase("Bigint=4&Decimal=88.000000000000&Float=5.5&"
                        + "Geog=POLYGON%20%28%280.0%200.0%2C%201.0%201.0%2C%200.0%201.0%2C%200.0%200.0%29%29&GeogPoint=POINT%20%28-122.0%2037.0%29&"
                        + "Integer=3&Smallint=2&String=x%20x&Timestamp=1970-01-01T00%3A00%3A00.000%2B0000&Tinyint=1&"
                        + "Signature=Xc%2BEKgpZel2Ypip35Zu4aoHtZW9HnYlDX3MhtfoVn20%3D")) {
                    success.set(true);
                } else {
                    System.err.println(body);
                }
            }
        });

        assertTrue(success.get());
    }

    @Test
    public void testFormSigningPrepareBodyWithNullValues() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();
        config.setProperty("type", "form");
        config.setProperty("signatureMethod", HttpExportClient.HmacSHA256);
        config.setProperty("signatureName", "Signature");
        config.setProperty("secret", "mysecretsecretkey");

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                final String body = EntityUtils.toString(((HttpEntityEnclosingRequest) httpRequest).getEntity());
                if (body.equalsIgnoreCase("Bigint=4&Decimal=88.000000000000&Float=%00&"
                        + "Geog=%00&GeogPoint=%00&Integer=3&Smallint=2&String=%00&Timestamp=%00&Tinyint=%00&"
                        + "Signature=UNOg1vHy%2BiDMni0mswYydI6qIIfjFDwlIMPcEPSH0QQ%3D")) {
                    success.set(true);
                } else {
                    System.err.println(body);
                }

            }
        }, m_testStartTime, true);

        assertTrue(success.get());
    }

    @Test
    public void testRequestHeader() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();
        config.setProperty("method", "post");
        config.setProperty("type", "form");

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("POST") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("application/x-www-form-urlencoded")) {
                    success.set(true);
                } else {
                    System.err.println(httpRequest.toString());
                }
            }
        });

        assertTrue(success.get());
    }

    @Test
    public void testBadResponse() throws Exception
    {
        final AtomicInteger count = new AtomicInteger(0);
        final Properties config = new Properties();
        config.setProperty("method", "post");
        config.setProperty("type", "form");

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                if (count.incrementAndGet() == 1) {
                    httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                    return;
                }
            }
        });

        assertEquals(2, count.get());
    }

    @Test
    public void testWebHdfsHeader() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();
        config.setProperty("method", "post");

        roundtripTest("/webhdfs/v1/root/%g/%t/%p.csv", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("POST") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("text/csv") &&
                    httpRequest.getHeaders("Expect")[0].getValue().startsWith("100-continue")) {
                    success.set(true);
                } else {
                    System.err.println(httpRequest.toString());
                }
            }
        });

        assertTrue(success.get());
    }

    @Test
    public void testWebHdfsRedirect() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();
        config.setProperty("method", "post");

        roundtripTest("/webhdfs/v1/root/%g/%t/%p.csv", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("POST") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("text/csv") &&
                    httpRequest.getHeaders("Expect")[0].getValue().startsWith("100-continue")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {
                        success.set(true);
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else {
                    System.err.println(httpRequest.toString());
                }
            }
        });

        assertTrue(success.get());
    }

    @Test
    public void testAvroRequest() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();
        final AtomicReference<byte[]> header = new AtomicReference<>();

        config.setProperty("avro.schema.location", new File(m_schemaOut,"%t.json").toString());
        config.setProperty("type", "avro");
        config.setProperty("avro.compress","true");

        roundtripTest("/webhdfs/v1/root/%g/%t/%p.avro", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                System.out.println("URI is: " + httpRequest.getRequestLine().getUri() + " Method is: " + httpRequest.getRequestLine().getMethod());
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type").length > 0 &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("avro/binary")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {

                        if (httpRequest instanceof HttpEntityEnclosingRequest) try {
                            HttpEntityEnclosingRequest put = (HttpEntityEnclosingRequest)httpRequest;
                            ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
                            put.getEntity().writeTo(baos);
                            header.set(baos.toByteArray());
                        } catch (Exception e) {
                           e.printStackTrace();
                           success.set(false);
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("POST") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("avro/binary") &&
                    httpRequest.getHeaders("Expect")[0].getValue().startsWith("100-continue")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    System.out.println(uri.toString() + " query:" + (uri.getQuery() == null ? "null" : uri.getQuery()));
                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {

                        if (httpRequest instanceof HttpEntityEnclosingRequest) try {
                            HttpEntityEnclosingRequest post = (HttpEntityEnclosingRequest)httpRequest;

                            Schema schema = new Schema.Parser().parse(new File(m_schemaOut,"yankeelover.json"));
                            GenericDatumReader<GenericRecord> ardr = new GenericDatumReader<>(schema);
                            ByteArrayOutputStream baos = new ByteArrayOutputStream(16 * 1024);
                            GenericRecord gr;

                            baos.write(header.get());
                            HttpEntity entity = post.getEntity();
                            entity.writeTo(baos);
                            DataFileReader<GenericRecord> fileReader =
                                    new DataFileReader<>(new SeekableByteArrayInput(baos.toByteArray()), ardr);
                            Iterator<GenericRecord> aitr = fileReader.iterator();

                            gr = aitr.next();
                            fileReader.close();
                            if (! new Integer(1).equals(gr.get("tinyint"))) {
                                System.err.println("expected tinyint value of 1, but gotten " + gr.get("tinyint"));
                                success.set(false);
                            } else {
                                success.set(true);
                            }
                        } catch (Exception e) {
                           e.printStackTrace();
                           success.set(false);
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else {
                    System.err.println(httpRequest.toString());
                }
            }
        });

        assertTrue(success.get());
    }

    @Test
    public void testAvroRequestOnHttpfs() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();
        final AtomicReference<byte[]> header = new AtomicReference<>();

        config.setProperty("avro.schema.location", new File(m_schemaOut,"%t.json").toString());
        config.setProperty("type", "avro");
        config.setProperty("avro.compress","true");
        config.setProperty("httpfs.enable","true");

        roundtripTest("/webhdfs/v1/root/%g/%t/%p.avro", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type").length > 0 &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("application/octet-stream")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {

                        if (httpRequest instanceof HttpEntityEnclosingRequest) try {
                            HttpEntityEnclosingRequest put = (HttpEntityEnclosingRequest)httpRequest;
                            ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
                            put.getEntity().writeTo(baos);
                            header.set(baos.toByteArray());
                        } catch (Exception e) {
                           e.printStackTrace();
                           success.set(false);
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("POST") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("application/octet-stream") &&
                    httpRequest.getHeaders("Expect")[0].getValue().startsWith("100-continue")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {

                        if (httpRequest instanceof HttpEntityEnclosingRequest) try {
                            HttpEntityEnclosingRequest post = (HttpEntityEnclosingRequest)httpRequest;

                            Schema schema = new Schema.Parser().parse(new File(m_schemaOut,"yankeelover.json"));
                            GenericDatumReader<GenericRecord> ardr = new GenericDatumReader<>(schema);
                            ByteArrayOutputStream baos = new ByteArrayOutputStream(16 * 1024);
                            GenericRecord gr;

                            baos.write(header.get());
                            HttpEntity entity = post.getEntity();
                            entity.writeTo(baos);
                            DataFileReader<GenericRecord> fileReader =
                                    new DataFileReader<>(new SeekableByteArrayInput(baos.toByteArray()), ardr);
                            Iterator<GenericRecord> aitr = fileReader.iterator();

                            if (!aitr.hasNext()) {
                                System.out.println("no next record found !!!");
                            }
                            gr = aitr.next();
                            fileReader.close();
                            if (! new Integer(1).equals(gr.get("tinyint"))) {
                                System.err.println("expected tinyint value of 1, but gotten " + gr.get("tinyint"));
                                success.set(false);
                            } else {
                                success.set(true);
                            }
                        } catch (Exception e) {
                           e.printStackTrace();
                           success.set(false);
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else {
                    System.err.println(httpRequest.toString());
                }
            }
        });

        assertTrue(success.get());
    }

    @Test
    public void testFileNotFound() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final AtomicInteger count = new AtomicInteger();
        final AtomicReference<byte[]> header = new AtomicReference<>();

        final Properties config = new Properties();

        config.setProperty("avro.schema.location", new File(m_schemaOut,"%t.json").toString());
        config.setProperty("contentType", "avro");

        roundtripTest("/webhdfs/v1/root/%t/%g/%p.avro", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type").length > 0 &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("avro/binary")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {

                        if (httpRequest instanceof HttpEntityEnclosingRequest) try {
                            HttpEntityEnclosingRequest put = (HttpEntityEnclosingRequest)httpRequest;
                            ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
                            put.getEntity().writeTo(baos);
                            header.set(baos.toByteArray());
                        } catch (Exception e) {
                           e.printStackTrace();
                           success.set(false);
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("POST") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("avro/binary") &&
                    httpRequest.getHeaders("Expect")[0].getValue().startsWith("100-continue")) {

                    if (count.incrementAndGet() == 1) {
                        HttpEntity enty = new StringEntity(
                                "{\"RemoteException\":{\"exception\":\"FileNotFoundException\",\"javaClassName\":\"java.io.FileNotFoundException\",\"message\":\"File /whatevah.\"}",
                                ContentType.APPLICATION_JSON
                                );
                        httpResponse.setEntity(enty);
                        httpResponse.setStatusCode(HttpStatus.SC_NOT_FOUND);
                        return;
                    }

                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {

                        if (httpRequest instanceof HttpEntityEnclosingRequest) try {
                            HttpEntityEnclosingRequest post = (HttpEntityEnclosingRequest)httpRequest;
                            ByteArrayOutputStream baos = new ByteArrayOutputStream(16 * 1024);

                            baos.write(header.get());
                            post.getEntity().writeTo(baos);

                            DataFileReader<GenericRecord> fileReader = new DataFileReader<>(
                                    new SeekableByteArrayInput(baos.toByteArray()),
                                    new GenericDatumReader<GenericRecord>()
                                    );

                            if (fileReader.getMetaString("avro.codec") != null) {
                                fileReader.close();
                                success.set(false);
                                System.err.println("incorrect value for the avro.code header");
                                return;
                            }

                            Iterator<GenericRecord> aitr = fileReader.iterator();

                            GenericRecord gr = aitr.next();
                            fileReader.close();

                            if (! new Integer(1).equals(gr.get("tinyint"))) {
                                System.err.println("expected tinyint value of 1, but gotten " + gr.get("tinyint"));
                                success.set(false);
                            } else {
                                success.set(true);
                            }
                        } catch (Exception e) {
                           e.printStackTrace();
                           success.set(false);
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else {
                    System.err.println(httpRequest.toString());
                }
            }
        });

        assertTrue(success.get());
    }

    @Test
    public void testFileAlreadyThere() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final AtomicBoolean renamed = new AtomicBoolean(false);
        final AtomicInteger count = new AtomicInteger();
        final AtomicReference<byte[]> header = new AtomicReference<>();

        final Properties config = new Properties();

        config.setProperty("avro.schema.location", new File(m_schemaOut,"%t.json").toString());
        config.setProperty("contentType", "avro");

        roundtripTest("/webhdfs/v1/root/%t/%g/%p.avro", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("GET") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {
                        if (uri.getQuery().contains("op=GETFILESTATUS")) {
                            HttpEntity enty = new StringEntity(
                                    "{\"FileStatus\":{\"accessTime\":1411746531849,\"blockSize\":134217728,\"childrenNum\":0,\"fileId\":16610," +
                                     "\"group\":\"supergroup\",\"length\":44,\"modificationTime\":1411746531857,\"owner\":\"root\"," +
                                     "\"pathSuffix\":\"\",\"permission\":\"755\",\"replication\":1,\"type\":\"FILE\"}}"
                                    );
                            httpResponse.setEntity(enty);
                            httpResponse.setStatusCode(HttpStatus.SC_OK);
                            return;
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {
                        if (httpRequest.getHeaders("Content-Type").length > 0 &&
                            httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("avro/binary")) {

                            if (count.incrementAndGet() == 1) {
                                HttpEntity enty = new StringEntity(
                                        "{\"RemoteException\":{\"exception\":\"FileAlreadyExistsException\",\"javaClassName\":\"org.apache.hadoop.fs.FileAlreadyExistsException\",\"message\":\"File /whatevah.\"}",
                                        ContentType.APPLICATION_JSON
                                        );
                                httpResponse.setEntity(enty);
                                httpResponse.setStatusCode(HttpStatus.SC_FORBIDDEN);
                                return;
                            }

                            if (httpRequest instanceof HttpEntityEnclosingRequest) try {
                                HttpEntityEnclosingRequest put = (HttpEntityEnclosingRequest)httpRequest;
                                ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
                                put.getEntity().writeTo(baos);
                                header.set(baos.toByteArray());
                            } catch (Exception e) {
                                e.printStackTrace();
                                success.set(false);
                            }
                        } else if (uri.getQuery().contains("op=RENAME")) {
                            renamed.set(true);
                        }

                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("POST") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("avro/binary") &&
                    httpRequest.getHeaders("Expect")[0].getValue().startsWith("100-continue")) {

                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {

                        if (httpRequest instanceof HttpEntityEnclosingRequest) try {
                            HttpEntityEnclosingRequest post = (HttpEntityEnclosingRequest)httpRequest;
                            ByteArrayOutputStream baos = new ByteArrayOutputStream(16 * 1024);

                            baos.write(header.get());
                            post.getEntity().writeTo(baos);

                            DataFileReader<GenericRecord> fileReader = new DataFileReader<>(
                                    new SeekableByteArrayInput(baos.toByteArray()),
                                    new GenericDatumReader<GenericRecord>()
                                    );
                            Iterator<GenericRecord> aitr = fileReader.iterator();

                            GenericRecord gr = aitr.next();
                            fileReader.close();

                            if (! new Integer(1).equals(gr.get("tinyint"))) {
                                System.err.println("expected tinyint value of 1, but gotten " + gr.get("tinyint"));
                                success.set(false);
                            } else {
                                success.set(true);
                            }
                        } catch (Exception e) {
                           e.printStackTrace();
                           success.set(false);
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else {
                    System.err.println(httpRequest.toString());
                }
            }
        });
        assertTrue(renamed.get());
        assertTrue(success.get());
    }

    @Test
    public void testFileAlreadyThereForHttpfs() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final AtomicBoolean renamed = new AtomicBoolean(false);
        final AtomicInteger count = new AtomicInteger();

        final Properties config = new Properties();

        config.setProperty("contentType", "csv");
        config.setProperty("httpfs.enable","true");

        roundtripTest("/webhdfs/v1/root/%t/%g/%p.csv", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("GET") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {
                        if (uri.getQuery().contains("op=GETFILESTATUS")) {
                            HttpEntity enty = new StringEntity(
                                    "{\"FileStatus\":{\"accessTime\":1411746531849,\"blockSize\":134217728,\"childrenNum\":0,\"fileId\":16610," +
                                     "\"group\":\"supergroup\",\"length\":44,\"modificationTime\":1411746531857,\"owner\":\"root\"," +
                                     "\"pathSuffix\":\"\",\"permission\":\"755\",\"replication\":1,\"type\":\"FILE\"}}"
                                    );
                            httpResponse.setEntity(enty);
                            httpResponse.setStatusCode(HttpStatus.SC_OK);
                            return;
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("application/octet-stream")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {
                        if (uri.getQuery().contains("op=CREATE") &&
                            httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("application/octet-stream")) {

                            if (count.incrementAndGet() == 1) {
                                HttpEntity enty = new StringEntity(
                                        "{\"RemoteException\":{\"exception\":\"FileAlreadyExistsException\",\"javaClassName\":\"org.apache.hadoop.fs.FileAlreadyExistsException\",\"message\":\"File /whatevah.\"}",
                                        ContentType.APPLICATION_JSON
                                        );
                                httpResponse.setEntity(enty);
                                httpResponse.setStatusCode(HttpStatus.SC_FORBIDDEN);
                                return;
                            }

                            if (httpRequest instanceof HttpEntityEnclosingRequest) try {
                                HttpEntityEnclosingRequest put = (HttpEntityEnclosingRequest)httpRequest;
                                if (put.getEntity().getContentLength() != 0) {
                                    success.set(false);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                success.set(false);
                            }
                        } else if (uri.getQuery().contains("op=RENAME")) {
                            renamed.set(true);
                        }

                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("POST") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("application/octet-stream") &&
                    httpRequest.getHeaders("Expect")[0].getValue().startsWith("100-continue")) {

                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {

                        if (httpRequest instanceof HttpEntityEnclosingRequest) try {
                            HttpEntityEnclosingRequest post = (HttpEntityEnclosingRequest)httpRequest;
                            success.set(post.getEntity().getContentLength() > 8);
                        } catch (Exception e) {
                           e.printStackTrace();
                           success.set(false);
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else {
                    System.err.println(httpRequest.toString());
                }
            }
        });
        assertTrue(renamed.get());
        assertTrue(success.get());
    }

    @Test
    public void testSetReplication() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);

        final Properties config = new Properties();
        config.setProperty("contentType", "avro");
        config.setProperty("replication", "1");

        roundtripTest("/webhdfs/v1/root/%t/%g/%p.avro", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                if(!(httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive"))){
                    System.err.println(httpRequest.toString());
                }else{
                    if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT")) {
                        final URI uri = URI.create(httpRequest.getRequestLine().getUri());
                        if (uri.getQuery() != null && uri.getQuery().contains("op=CREATE") && uri.getQuery().contains("replication")) {
                            success.set(true);
                        }
                    }
                }
            }
        });
        assertTrue(success.get());
    }

    @Test
    public void testSetReplicationAfterAppendFailure() throws Exception
    {
        final AtomicBoolean success = new AtomicBoolean(false);
        final AtomicInteger count = new AtomicInteger();
        final AtomicReference<byte[]> header = new AtomicReference<>();

        final Properties config = new Properties();
        config.setProperty("contentType", "avro");

        roundtripTest("/webhdfs/v1/root/%t/%g/%p.avro", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
               if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());
                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {
                        if (httpRequest.getHeaders("Content-Type").length > 0 &&
                            httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("avro/binary")) {
                          if (httpRequest instanceof HttpEntityEnclosingRequest) {
                                try {
                                  HttpEntityEnclosingRequest put = (HttpEntityEnclosingRequest)httpRequest;
                                  ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
                                  put.getEntity().writeTo(baos);
                                  header.set(baos.toByteArray());
                               } catch (Exception e) {
                                  e.printStackTrace();
                                  success.set(false);
                               }
                            }
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("POST") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive") &&
                    httpRequest.getHeaders("Content-Type")[0].getValue().startsWith("avro/binary") &&
                    httpRequest.getHeaders("Expect")[0].getValue().startsWith("100-continue")) {

                    if (count.incrementAndGet() == 1) {
                        HttpEntity enty = new StringEntity(
                                "{\"RemoteException\":{\"exception\":\"RecoveryInProgressException\",\"javaClassName\":\"org.apache.hadoop.hdfs.protocol.RecoveryInProgressException\",\"message\":\"File /whatevah.\"}",
                                ContentType.APPLICATION_JSON
                                );
                        httpResponse.setEntity(enty);
                        httpResponse.setStatusCode(HttpStatus.SC_FORBIDDEN);
                        return;
                    }
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());
                    if (uri.getQuery() != null && uri.getQuery().contains("redirected")) {

                        if (httpRequest instanceof HttpEntityEnclosingRequest){
                            try {
                               HttpEntityEnclosingRequest post = (HttpEntityEnclosingRequest)httpRequest;
                               ByteArrayOutputStream baos = new ByteArrayOutputStream(16 * 1024);

                               baos.write(header.get());
                               post.getEntity().writeTo(baos);

                               DataFileReader<GenericRecord> fileReader = new DataFileReader<>(
                                    new SeekableByteArrayInput(baos.toByteArray()),
                                    new GenericDatumReader<GenericRecord>()
                                    );
                               Iterator<GenericRecord> aitr = fileReader.iterator();

                               GenericRecord gr = aitr.next();
                               fileReader.close();

                               if (! new Integer(1).equals(gr.get("tinyint"))) {
                                  System.err.println("expected tinyint value of 1, but gotten " + gr.get("tinyint"));
                                  success.set(false);
                                } else {
                                   success.set(true);
                                }
                            } catch (Exception e) {
                               success.set(false);
                           }
                        }
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else {
                    System.err.println(httpRequest.toString());
                }
            }
        });
        assertTrue(success.get());
    }

    @Test
    public void testRoll() throws Exception
    {
        final int rolls = 5;
        final int rollTimePeriodSecs = 1;
        Semaphore received = new Semaphore(0);

        final Properties config = new Properties();
        config.setProperty("period", rollTimePeriodSecs + "s");
        String endpoint = "/webhdfs/v1/root/%d/%t/%g/%p.csv";

        HttpRequestHandler requestHandler = new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException
            {
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT") &&
                    httpRequest.getHeaders("Connection")[0].getValue().startsWith("Keep-Alive")) {
                    final URI uri = URI.create(httpRequest.getRequestLine().getUri());

                    if (uri.getQuery().contains("op=MKDIRS")) {
                        received.release();
                    } else if (uri.getQuery().contains("redirected")) {
                        //no op
                    } else {
                        httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                        httpResponse.setHeader("Location", uri.toString() + "?redirected");
                    }
                } else {
                    System.err.println(httpRequest.toString());
                }
            }
        };
        boolean success = false;
        for (int i = 0; i < rolls; ++i) {
            roundtripTest(endpoint, config, requestHandler, m_testStartTime+i, (i % 2) == 0);
            success = received.tryAcquire(rollTimePeriodSecs * 2, TimeUnit.SECONDS);
            assertTrue("nothing received", success);
        }
    }

    private void populateTable(long value, boolean useNullValues) {
        if (useNullValues) {
            m_table.addRow(value, value, value, DEFAULT_PARTITION_ID, OPERATION, value,
                    VoltType.NULL_TINYINT,
                    /* partitioning column */ (short) 2,
                    3, 4, VoltType.NULL_FLOAT, VoltType.NULL_TIMESTAMP, VoltType.NULL_STRING_OR_VARBINARY, new BigDecimal(88),
                    VoltType.NULL_POINT, VoltType.NULL_GEOGRAPHY);
        } else {
            m_table.addRow(value, value, value, DEFAULT_PARTITION_ID, 1, value,
                    (byte) 1,
                    /* partitioning column */ (short) 2,
                    3, 4, 5.5, 6, "x x", new BigDecimal(88),
                    GEOG_POINT, GEOG);
        }
        m_table.advanceRow();
    }

    protected void roundtripTest(final String endpointPath, final Properties config, final HttpRequestHandler handler) throws Exception {
        roundtripTest(endpointPath, config, handler, m_testStartTime, false);
    }

    protected void roundtripTest(final String endpointPath, final Properties config, final HttpRequestHandler handler, long headerValue, boolean useNullValues) throws Exception {
        setupServer();
        m_server.register("*", handler);

        final HttpExportClient dut = new HttpExportClient();
        config.setProperty("endpoint", "http:/" + m_server.getServiceAddress().toString() + endpointPath);
        if (config.getProperty("period") == null) {
            config.setProperty("period", "1s"); // must set else it defaults to 1 hour
        }
        dut.configure(config);

        final ExportDecoderBase decoder = dut.constructExportDecoder(constructTestSource(false, 0));
        populateTable(headerValue, useNullValues);
        byte[] bufBytes = ExportEncoder.encodeRow(m_table, "yankeelover", 7, 32L);

        ByteBuffer bb = ByteBuffer.wrap(bufBytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        int schemaSize = bb.getInt();
        ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
        int size = bb.getInt(); // row size
        byte [] rowBytes = new byte[size];
        bb.get(rowBytes);
        while (true) {
            try {
                ExportRow row = ExportRow.decodeRow(schemaRow, 0, rowBytes);
                decoder.onBlockStart(row);
                decoder.processRow(row);
                decoder.onBlockCompletion(row);
                break;
            } catch (ExportDecoderBase.RestartBlockException e) {
                assertTrue(e.requestBackoff);
            }
        }

    }
}
