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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.voltdb.exportclient.ElasticSearchHttpExportClient.HttpExportDecoder;
import org.voltdb.exportclient.decode.EndpointExpander;
import org.voltdb.types.TimestampType;

public class TestElasticSearchHttpExportClient extends ExportClientTestBase
{
    static File schemaOut;
    LocalTestServer server = null;

    void setupServer() throws Exception {
        server = new LocalTestServer(null, null);
        server.start();
    }

    @BeforeClass
    public static void setupClass() throws Exception {

    }

    @AfterClass
    public static void tearDownClass() throws Exception {

    }

    @Override
    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void tearDown() throws Exception {
        if (server != null)
        {
            server.stop();
        }
        server = null;
    }

    @Test
    public void testEmptyConfigValidation() throws Exception {
        final ElasticSearchHttpExportClient dut = new ElasticSearchHttpExportClient();
        final Properties config = new Properties();
        try
        {
            dut.configure(config);
            fail("Empty config");
        } catch (Exception e)
        {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("must provide an endpoint"));
        }
    }

    @Test
    public void testSchemeValidation() throws Exception {
        final ElasticSearchHttpExportClient dut = new ElasticSearchHttpExportClient();
        final Properties config = new Properties();
        URI dutURI = null;

        // Wrong scheme
        config.setProperty("endpoint", "htttp://fakehost");
        try
        {
            dut.configure(config);
            fail("Wrong scheme");
        } catch (Exception e)
        {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("could not expand"));
        }

        // Correct schemes
        config.setProperty("endpoint", "http://fakehost/%t/%p");
        dut.configure(config);
        dutURI = new URI(EndpointExpander.expand(dut.m_endpoint, "SAMPLE_T", 1,
                123L, new Date()));
        assertEquals("http", dutURI.getScheme());

        config.setProperty("endpoint", "https://fakehost/%t/%g/%p");
        dut.configure(config);
        dutURI = new URI(EndpointExpander.expand(dut.m_endpoint, "SAMPLE_T", 1,
                123L, new Date()));
        assertEquals("https", dutURI.getScheme());
    }

    @Test
    public void testRequestHeader() throws Exception {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest,
                    HttpResponse httpResponse, HttpContext httpContext)
                            throws HttpException, IOException {
                if (httpRequest.getRequestLine().getMethod()
                        .equalsIgnoreCase("POST")
                        && httpRequest.getHeaders("Connection")[0].getValue()
                        .startsWith("Keep-Alive")
                        && httpRequest.getHeaders("Content-Type")[0].getValue()
                        .startsWith("application/json"))
                {
                    success.set(true);
                } else
                {
                    System.err.println(httpRequest.toString());
                }
            }
        });

        assertTrue(success.get());
    }

    @Test
    public void testBadResponse() throws Exception {
        final AtomicInteger count = new AtomicInteger(0);
        final Properties config = new Properties();
        config.setProperty("method", "post");

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest,
                    HttpResponse httpResponse, HttpContext httpContext)
                            throws HttpException, IOException {
                if (count.incrementAndGet() == 1)
                {
                    httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                    return;
                }
            }
        });

        assertEquals(2, count.get());
    }

    @Test
    public void testBadBulkResponse() throws Exception {
        final AtomicInteger count = new AtomicInteger(0);
        final Properties config = new Properties();
        config.setProperty("batch.mode", "true");

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest,
                    HttpResponse httpResponse, HttpContext httpContext)
                            throws HttpException, IOException {
                if (count.incrementAndGet() == 1)
                {
                    String rspBody = "{\n" + "\"took\": 2,\n"
                            + "\"errors\": true,\n" + "\"items\": [\n" + "{\n"
                            + "\"create\": {\n" + "\"_index\": \"customer\",\n"
                            + "\"_type\": \"external\",\n"
                            + "\"_id\": \"AU4wh8cQakyrZmL-tl6X\",\n"
                            + "\"_version\": 1,\n" + "\"status\": 201\n" + "}\n"
                            + "},\n" + "{\n" + "\"create\": {\n"
                            + "\"_index\": \"new\",\n"
                            + "\"_type\": \"external\",\n"
                            + "\"_id\": \"AU4wh8cQakyrZmL-tl6Y\",\n"
                            + "\"_version\": 1,\n" + "\"status\": 401\n" + "}\n"
                            + "}\n" + "]\n" + "}\n";
                    HttpEntity enty = new StringEntity(rspBody, ContentType.APPLICATION_JSON);
                    httpResponse.setEntity(enty);
                    httpResponse.setStatusCode(HttpStatus.SC_OK);
                    return;
                }
            }
        });

        assertEquals(2, count.get());
    }

    @Test
    public void testBulkPrepareBody() throws Exception {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest,
                    HttpResponse httpResponse, HttpContext httpContext)
                            throws HttpException, IOException {
                final String body = EntityUtils.toString(((HttpEntityEnclosingRequest) httpRequest).getEntity());
                if (body.startsWith("{\"index\":{}}\n"+
                        "{\"tinyint\":1,\"smallint\":2,\"integer\":3,\"bigint\":4,\"float\":5.5")){
                    success.set(true);
                } else {
                    System.err.println(body);
                }
            }
        });

        assertTrue(success.get());
    }

    @Test
    public void testIndexPrepareBody() throws Exception {
        final AtomicBoolean success = new AtomicBoolean(false);
        final Properties config = new Properties();
        config.setProperty("batch.mode", "false");

        roundtripTest("/%p/%t", config, new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest httpRequest,
                    HttpResponse httpResponse, HttpContext httpContext)
                            throws HttpException, IOException {
                final String body = EntityUtils.toString(((HttpEntityEnclosingRequest) httpRequest).getEntity());
                if (body.startsWith("{\"tinyint\":1,\"smallint\":2,\"integer\":3,\"bigint\":4,\"float\":5.5")){
                    success.set(true);
                } else {
                    System.err.println(body);
                }
            }
        });

        assertTrue(success.get());
    }

    protected void roundtripTest(final String endpointPath,
            final Properties config, final HttpRequestHandler handler)
                    throws Exception {
        setupServer();
        server.register("*", handler);

        final ElasticSearchHttpExportClient dut = new ElasticSearchHttpExportClient();
        config.setProperty("endpoint", "http:/"
                + server.getServiceAddress().toString() + endpointPath);
        dut.configure(config);

        final ExportDecoderBase decoder = dut
                .constructExportDecoder(constructTestSource(false, 0));

        final HttpExportDecoder dec = (HttpExportDecoder) decoder;
        assert(dec.getExportPath() == null);

        long l = System.currentTimeMillis();
        vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                /* partitioning column */(short) 2, 3, 4, 5.5, new TimestampType(
                        new Date()), "x x", new BigDecimal(88), GEOG_POINT, GEOG);
        vtable.advanceRow();
        byte[] bufBytes = ExportEncoder.encodeRow(vtable,
                "mytable",
                0,
                1L);

        ByteBuffer bb = ByteBuffer.wrap(bufBytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        int schemaSize = bb.getInt();
        ExportRow schemaRow = ExportRow.decodeBufferSchema(bb, schemaSize, 1, 0);
        int size = bb.getInt(); // row size
        byte [] rowBytes = new byte[size];
        bb.get(rowBytes);
        while (true) {
            try {
                ExportRow r = ExportRow.decodeRow(schemaRow, 0, rowBytes);
                decoder.onBlockStart(r);
                decoder.processRow(r);
                decoder.onBlockCompletion(r);
                break;
            }
            catch (ExportDecoderBase.RestartBlockException e) {
                assertTrue(e.requestBackoff);
            }
        }

    }
}
