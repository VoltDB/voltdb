package org.voltcore.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.localserver.LocalTestServer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltdb.exportclient.ExportClientTestBase;

public class TestHDFSUtils extends ExportClientTestBase {
    VoltLogger m_logger = new VoltLogger("HDFSUtils");
    LocalTestServer responseServer = null;
    LocalTestServer redirectServer = null;

    private void setupResponseServer() throws Exception {
        responseServer = new LocalTestServer(null, null);
        responseServer.start();
    }

    private void setupRedirectServer() throws Exception {
        redirectServer = new LocalTestServer(null, null);
        redirectServer.start();
    }

    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void tearDown() throws Exception {
        if (responseServer != null) {
            responseServer.stop();
        }
        if (redirectServer != null) {
            redirectServer.stop();
        }
        redirectServer = null;
        responseServer = null;
    }

    @Test
    public void testCreateFile() throws Exception{
        setupResponseServer();
        setupRedirectServer();
        redirectServer.register("*", new HttpRequestHandler(){
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse,
                    HttpContext httpContext) throws HttpException, IOException {
                URI requestURI = null;
                try {
                    requestURI = new URI(httpRequest.getRequestLine().getUri());
                } catch (URISyntaxException e) {
                    m_logger.error("URI request error", e);
                }
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT") &&
                        requestURI.getQuery() != null && requestURI.getQuery().contains("op=CREATE")) {
                    httpResponse.setStatusCode(HttpStatus.SC_TEMPORARY_REDIRECT);
                    String redirectURL = "http:/" + responseServer.getServiceAddress().toString() + "/?op=CREATE";
                    httpResponse.setHeader("Location", redirectURL);
                }
                else {
                    httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                }
            }
        });
        responseServer.register("*", new HttpRequestHandler(){
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse,
                    HttpContext httpContext) throws HttpException, IOException {
                URI requestURI = null;
                try {
                    requestURI = new URI(httpRequest.getRequestLine().getUri());
                } catch (URISyntaxException e) {
                    m_logger.error("URI request error", e);
                }
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT") &&
                        requestURI.getQuery() != null && requestURI.getQuery().contains("op=CREATE")) {
                    httpResponse.setStatusCode(HttpStatus.SC_OK);
                }
                else {
                    httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                }
            }
        });
        HttpClient client = HttpClientBuilder.create().build();
        URI url = new URI("http:/" + redirectServer.getServiceAddress().toString());
        HDFSUtils hdfs = new HDFSUtils(m_logger);
        Boolean success = hdfs.createFile(url, client);
        assertTrue(success);
    }

    @Test
    public void testCreateDir() throws Exception{
        setupResponseServer();
        responseServer.register("*", new HttpRequestHandler(){
            @Override
            public void handle(HttpRequest httpRequest, HttpResponse httpResponse,
                    HttpContext httpContext) throws HttpException, IOException {
                URI requestURI = null;
                try {
                    requestURI = new URI(httpRequest.getRequestLine().getUri());
                } catch (URISyntaxException e) {
                    m_logger.error("URI request error", e);
                }
                if (httpRequest.getRequestLine().getMethod().equalsIgnoreCase("PUT") &&
                        requestURI.getQuery() != null && requestURI.getQuery().contains("op=MKDIRS")) {
                    httpResponse.setStatusCode(HttpStatus.SC_OK);
                }
                else {
                    httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                }
            }
        });
        HttpClient client = HttpClientBuilder.create().build();
        URI url = new URI("http:/" + responseServer.getServiceAddress().toString());
        HDFSUtils hdfs = new HDFSUtils(m_logger);
        Boolean success = hdfs.createDirectory(url, client);
        assertTrue(success);
    }
}
