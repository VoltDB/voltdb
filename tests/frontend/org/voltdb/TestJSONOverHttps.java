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
/*
 Copyright (c) 2008 Twilio, Inc.

 Permission is hereby granted, free of charge, to any person
 obtaining a copy of this software and associated documentation
 files (the "Software"), to deal in the Software without
 restriction, including without limitation the rights to use,
 copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the
 Software is furnished to do so, subject to the following
 conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * What's up with the Twilio stuff? They have MIT licensed
 * REST clients in lots of languages, even Java. It's not a
 * direct copy/paste, but this code borrows heavily from what
 * they did in a few ways.
 * Thanks Twilio!
 */
package org.voltdb;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import javax.net.ssl.SSLContext;

import junit.framework.TestCase;
import org.apache.http.Header;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestJSONOverHttps extends TestCase {
    private static final ContentType utf8ApplicationFormUrlEncoded =
            ContentType.create("application/x-www-form-urlencoded","UTF-8");
    private static final int HTTPD_PORT = 8095;
    private static final String KEYSTORE_RESOURCE = "keystore";
    private static final String KEYSTORE_PASSWD = "password";
    private static final String KEYSTORE_PASSWD_OBFUSCATED = "OBF:1v2j1uum1xtv1zej1zer1xtn1uvk1v1v";
    private static final String KEYSTORE_SYSPROP = "javax.net.ssl.keyStore";
    private static final String KEYSTORE_PASSWD_SYSPROP = "javax.net.ssl.keyStorePassword";
    private static final String TRUSTSTORE_SYSPROP = "javax.net.ssl.trustStore";
    private static final String TRUSTSTORE_PASSWD_SYSPROP = "javax.net.ssl.trustStorePassword";

    private ServerThread m_server;
    private int m_port;

    private String callProcOverJSON(String varString, final int expectedCode) throws Exception {
        URI uri = URI.create("https://localhost:" + m_port + "/api/1.0/");
        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build();
        SSLConnectionSocketFactory sf = new SSLConnectionSocketFactory(sslContext,
          SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", sf)
                .build();

        // allows multi-threaded use
        PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager( socketFactoryRegistry);

        HttpClientBuilder b = HttpClientBuilder.create();
        b.setSslcontext(sslContext);
        b.setConnectionManager(connMgr);

        try (CloseableHttpClient httpclient = b.build()) {
            HttpPost post = new HttpPost(uri);
            // play nice by using HTTP 1.1 continue requests where the client sends the request headers first
            // to the server to see if the server is willing to accept it. This allows us to test large requests
            // without incurring server socket connection terminations
            RequestConfig rc = RequestConfig.copy(RequestConfig.DEFAULT).setExpectContinueEnabled(true).build();
            post.setProtocolVersion(HttpVersion.HTTP_1_1);
            post.setConfig(rc);
            post.setEntity(new StringEntity(varString, utf8ApplicationFormUrlEncoded));
            ResponseHandler<String> rh = new ResponseHandler<String>() {
                @Override
                public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    assertEquals(expectedCode, status);
                    if (status == 200) {
                        //If we got a good response we should have a cookie. Some good responses dont have cookie so check header present.
                        Header hs[] = response.getHeaders("Set-Cookie");
                        if (hs != null && hs.length > 0) {
                            String cookie = hs[0].getValue();
                            assertTrue(cookie.contains("vmc"));
                            assertTrue(cookie.contains("HttpOnly"));
                            //For SSL we look for Secure flag on cookie
                            assertTrue(cookie.contains("Secure"));
                        }
                    }
                    if ((status >= 200 && status < 300) || status == 400) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    }
                    return null;
                }
            };
            return httpclient.execute(post,rh);
        }
    }

    private void startServer(String keyStorePath, String keyStorePasswd,
                             String certStorePath, String certStorePasswd) throws IOException {
        m_port = HTTPD_PORT;
        startServer(keyStorePath, keyStorePasswd, certStorePath, certStorePasswd, HTTPD_PORT);
    }
    private void startServer(String keyStorePath, String keyStorePasswd,
                             String certStorePath, String certStorePasswd,
                             int port) throws IOException {
            String testSchema
                    = "CREATE TABLE foo (\n"
                    + "  ival bigint default 23 not null, "
                    + "  sval varchar(200) default 'foo', "
                    + "  PRIMARY KEY (ival)\n"
                    + ");";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(testSchema);
            builder.addPartitionInfo("foo", "ival");
            builder.addStmtProcedure("fooinsert", "insert into foo values (?, ?);");
            builder.addStmtProcedure("foocount", "select count(*) from foo;");
            builder.setHTTPDPort(port);
            builder.setSslEnabled(true);
            builder.setSslExternal(true);
            if (keyStorePath != null) {
                String keystore = getResourcePath(keyStorePath);
                builder.setKeyStoreInfo(keystore, keyStorePasswd);
            }
            if (certStorePath != null) {
                String certstore = getResourcePath(certStorePath);
                builder.setCertStoreInfo(certstore, certStorePasswd);
            }
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("json.jar"));
            assertTrue(success);

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = config.setPathToCatalogForTest("json.jar");
            config.m_pathToDeployment = builder.getPathToDeployment();
            m_server = new ServerThread(config);
            m_server.start();
            m_server.waitForInitialization();
    }

    private String getResourcePath(String resource) {
        URL res = this.getClass().getResource(resource);
        return res==null ? resource : res.getPath();
    }

    /* To obtain the obfuscated password, use jetty utility as shown below:
     * Manjus-MacBook-Pro:voltdb manjujames$ java -cp lib/jetty-util-9.3.6.v20151106.jar org.eclipse.jetty.util.security.Password password
     * 2016-04-07 15:40:26.608:INFO::main: Logging initialized @102ms
     * password
     * OBF:1v2j1uum1xtv1zej1zer1xtn1uvk1v1v
     * MD5:5f4dcc3b5aa765d61d8327deb882cf99
     * Manjus-MacBook-Pro:voltdb manjujames$
     */
    public void testObfuscatedPassword() throws Exception {
        try {
            System.setProperty(KEYSTORE_SYSPROP, "");
            System.setProperty(KEYSTORE_PASSWD_SYSPROP, "");
            System.setProperty(TRUSTSTORE_SYSPROP, "");
            System.setProperty(TRUSTSTORE_PASSWD_SYSPROP, "");
            startServer(KEYSTORE_RESOURCE, KEYSTORE_PASSWD_OBFUSCATED, KEYSTORE_RESOURCE, KEYSTORE_PASSWD_OBFUSCATED);

            String varString = "Procedure=foocount";
            TestJSONInterface.Response response =
                    TestJSONInterface.responseFromJSON(callProcOverJSON(varString, 200));
            VoltTable result = response.results[0];
            result.advanceRow();
            assertEquals(0, result.getLong(0));
        } finally {
            if (m_server != null) {
                m_server.shutdown();
                m_server.join();
            }
            m_server = null;
        }
    }

    public void testKeystoreInDeployment() throws Exception {
        try {
            System.setProperty(KEYSTORE_SYSPROP, "");
            System.setProperty(KEYSTORE_PASSWD_SYSPROP, "");
            System.setProperty(TRUSTSTORE_SYSPROP, "");
            System.setProperty(TRUSTSTORE_PASSWD_SYSPROP, "");
            startServer(KEYSTORE_RESOURCE, KEYSTORE_PASSWD, KEYSTORE_RESOURCE, KEYSTORE_PASSWD);

            String varString = "Procedure=foocount";
            TestJSONInterface.Response response =
                    TestJSONInterface.responseFromJSON(callProcOverJSON(varString, 200));
            VoltTable result = response.results[0];
            result.advanceRow();
            assertEquals(0, result.getLong(0));
        } finally {
            if (m_server != null) {
                m_server.shutdown();
                m_server.join();
            }
            m_server = null;
        }
    }

    public void testDefaultPortDeployment() throws Exception {
        try {
            System.setProperty(KEYSTORE_SYSPROP, "");
            System.setProperty(KEYSTORE_PASSWD_SYSPROP, "");
            System.setProperty(TRUSTSTORE_SYSPROP, "");
            System.setProperty(TRUSTSTORE_PASSWD_SYSPROP, "");
            m_port = VoltDB.DEFAULT_HTTPS_PORT;
            startServer(KEYSTORE_RESOURCE, KEYSTORE_PASSWD, KEYSTORE_RESOURCE, KEYSTORE_PASSWD, 0);

            String varString = "Procedure=foocount";
            TestJSONInterface.Response response =
                    TestJSONInterface.responseFromJSON(callProcOverJSON(varString, 200));
            VoltTable result = response.results[0];
            result.advanceRow();
            assertEquals(0, result.getLong(0));
        } finally {
            if (m_server != null) {
                m_server.shutdown();
                m_server.join();
            }
            m_server = null;
        }
    }

    public void testKeystoreCertStoreInDeployment() throws Exception {
        try {
            System.setProperty(KEYSTORE_SYSPROP, "");
            System.setProperty(KEYSTORE_PASSWD_SYSPROP, "");
            System.setProperty(TRUSTSTORE_SYSPROP, "");
            System.setProperty(TRUSTSTORE_PASSWD_SYSPROP, "");
            startServer(KEYSTORE_RESOURCE, KEYSTORE_PASSWD, KEYSTORE_RESOURCE, KEYSTORE_PASSWD);

            String varString = "Procedure=foocount";
            TestJSONInterface.Response response =
                    TestJSONInterface.responseFromJSON(callProcOverJSON(varString, 200));
            VoltTable result = response.results[0];
            result.advanceRow();
            assertEquals(0, result.getLong(0));
        } finally {
            if (m_server != null) {
                m_server.shutdown();
                m_server.join();
            }
            m_server = null;
        }
    }

    public void testKeystoreCertStoreInSysProps() throws Exception {
        try {
            System.setProperty(KEYSTORE_SYSPROP, getResourcePath(KEYSTORE_RESOURCE));
            System.setProperty(KEYSTORE_PASSWD_SYSPROP, KEYSTORE_PASSWD);
            System.setProperty(TRUSTSTORE_SYSPROP, getResourcePath(KEYSTORE_RESOURCE));
            System.setProperty(TRUSTSTORE_PASSWD_SYSPROP, KEYSTORE_PASSWD);
            startServer("invalid", "invalid", null, null);

            String varString = "Procedure=foocount";
            TestJSONInterface.Response response =
                    TestJSONInterface.responseFromJSON(callProcOverJSON(varString, 200));
            VoltTable result = response.results[0];
            result.advanceRow();
            assertEquals(0, result.getLong(0));
        } finally {
            if (m_server != null) {
                m_server.shutdown();
                m_server.join();
            }
            m_server = null;
        }
    }
}
