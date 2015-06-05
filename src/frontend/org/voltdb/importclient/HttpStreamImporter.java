/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.ProtocolException;
import org.apache.http.annotation.Immutable;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.importer.CSVInvocation;
import org.voltdb.importer.ImportHandlerProxy;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Implement a BundleActivator interface and extend ImportHandlerProxy.
 */
public class HttpStreamImporter extends ImportHandlerProxy implements BundleActivator {
    private static final int HTTP_EXPORT_MAX_CONNS = Integer.getInteger("HTTP_EXPORT_MAX_CONNS", 20);
    public static final Header OctetStreamContentTypeHeader =
            new BasicHeader("Content-Type", ContentType.APPLICATION_OCTET_STREAM.getMimeType());

    private Properties m_properties;
    private CloseableHttpAsyncClient m_client = HttpAsyncClients.createDefault();
    private PoolingNHttpClientConnectionManager m_connManager = null;
    private String m_procedure;
    private URI m_endpoint;

    // Register ImportHandlerProxy service.
    @Override
    public void start(BundleContext context) throws Exception {
        context.registerService(ImportHandlerProxy.class.getName(), this, null);
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        //Do any bundle related cleanup.
    }

    @Override
    public void stop() {
        try {
            m_client.close();
            m_connManager.shutdown(60 * 1000);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Return a name for VoltDB to log with friendly name.
     * @return name of the importer.
     */
    @Override
    public String getName() {
        return "HttpImporter";
    }

    /**
     * Construct an async HTTP client with connection pool.
     * @throws IOReactorException
     */
    private void connect() throws IOReactorException
    {
        if (m_connManager == null) {
            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
            m_connManager = new PoolingNHttpClientConnectionManager(ioReactor);
            m_connManager.setMaxTotal(HTTP_EXPORT_MAX_CONNS);
            m_connManager.setDefaultMaxPerRoute(HTTP_EXPORT_MAX_CONNS);
        }

        if (m_client == null || !m_client.isRunning()) {
            HttpAsyncClientBuilder client = HttpAsyncClients.custom().setConnectionManager(m_connManager).
                    setRedirectStrategy(new HadoopRedirectStrategy());
            m_client = client.build();
            m_client.start();
        }
    }

    private void makeReadDir() throws PathHandlingException {
        DecodedStatus status = DecodedStatus.FAIL;
        try {
            HttpPut dirMaker = new HttpPut(new URI(m_endpoint.getScheme(), m_endpoint.getAuthority(),
                    m_endpoint.getPath() + "/TmpReadDir", "op=MKDIRS", m_endpoint.getFragment()));
            HttpResponse resp = m_client.execute(dirMaker,null).get();
            status = DecodedStatus.fromResponse(resp);
            if (status != DecodedStatus.OK) error(status.toString());
        } catch (InterruptedException|ExecutionException|URISyntaxException e) {
            //rateLimitedLogError(m_logger, "error creating parent directory for %s %s", path, Throwables.getStackTraceAsString(e));
            throw new PathHandlingException("error creating parent directory for " + m_endpoint.toString(), e);
        }
    }

    /**
     * This is called with the properties that are supplied in the deployment.xml
     * Do any initialization here.
     * @param p
     */
    @Override
    public void configure(Properties p) {
        m_properties = (Properties) p.clone();
        try {
            m_endpoint = new URI((String)m_properties.get("endpoint"));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        m_procedure = (String )m_properties.get("procedure");
        if (m_procedure == null || m_procedure.trim().length() == 0) {
            throw new RuntimeException("Missing procedure.");
        }
        try {
            connect();
            makeReadDir();
        } catch (IOException ex) {
           ex.printStackTrace();
           throw new RuntimeException(ex.getCause());
        }
    }

    /**
     * This is called when server is ready to accept any transactions.
     */
    @Override
    public void readyForData() {
        try {
            info("Configured and ready with properties: " + m_properties);
            while (true) {
                HttpGet checkDir = new HttpGet(new URI(m_endpoint.getScheme(), m_endpoint.getAuthority(),
                        m_endpoint.getPath(), "op=LISTSTATUS", m_endpoint.getFragment()));
                HttpResponse resp = m_client.execute(checkDir,null).get();
                DecodedStatus status = DecodedStatus.fromResponse(resp);
                if (status != DecodedStatus.OK) error(status.toString());
                else {
                    JSONObject result = new JSONObject(EntityUtils.toString(resp.getEntity()));
                    JSONArray dirList = result.getJSONObject("FileStatuses").getJSONArray("FileStatus");
                    for(int i=0; i < dirList.length(); i++) {
                        JSONObject entry = dirList.getJSONObject(i);
                        if ("FILE".equals(entry.getString("type"))) {
                            String pathSuffix = entry.getString("pathSuffix");
                            if (takeImportFile(pathSuffix)) {
                                String filePath = m_endpoint.getPath() + "/TmpReadDir/" + entry.getString("pathSuffix");
                                ingestFile(filePath);
                            }
                        }
                    }
                }
                Thread.sleep(Integer.parseInt(m_properties.getProperty("delay")) * 1000);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private Boolean takeImportFile(String pathSuffix) throws PathHandlingException {
        try {
            info("m_endpoint.getPath().split('v1')[1] = " + m_endpoint.getPath().split("v1")[1]);
            HttpPut dirMaker = new HttpPut(new URI(m_endpoint.getScheme(), m_endpoint.getAuthority(),
                    m_endpoint.getPath() + "/" + pathSuffix, "op=RENAME&destination=" + m_endpoint.getPath().split("v1")[1] +
                    "/TmpReadDir/" + pathSuffix, m_endpoint.getFragment()));
            HttpResponse resp = m_client.execute(dirMaker,null).get();
            DecodedStatus status = DecodedStatus.fromResponse(resp);
            if (status != DecodedStatus.OK) {
                error(status.toString());
                return false;
            }
            else {
                return new JSONObject(EntityUtils.toString(resp.getEntity())).getBoolean("boolean");
            }
        } catch (InterruptedException|ExecutionException|URISyntaxException | ParseException | JSONException | IOException e) {
            //rateLimitedLogError(m_logger, "error creating parent directory for %s %s", path, Throwables.getStackTraceAsString(e));
            throw new PathHandlingException("error creating parent directory for " + m_endpoint.getPath(), e);
        }
    }

    private void ingestFile(String filePath) throws PathHandlingException {
        try {
            HttpGet fileRead = new HttpGet(new URI(m_endpoint.getScheme(), m_endpoint.getAuthority(),
                    filePath, "op=OPEN", m_endpoint.getFragment()));
            HttpResponse resp = m_client.execute(fileRead,null).get();
            DecodedStatus status = DecodedStatus.fromResponse(resp);
            if (status != DecodedStatus.OK) error(status.toString());
            else {
                BufferedReader in = new BufferedReader(new InputStreamReader(resp.getEntity().getContent()));
                while (true) {
                    String line = in.readLine();
                    //You should convert your data to params here.
                    if (line == null) break;
                    CSVInvocation invocation = new CSVInvocation(m_procedure, line);
                    if (!callProcedure(this, invocation)) {
                        System.out.println("Inserted failed: " + line);
                    }
                }
            }
        } catch (InterruptedException|ExecutionException|URISyntaxException | ParseException | IOException e) {
            //rateLimitedLogError(m_logger, "error creating parent directory for %s %s", path, Throwables.getStackTraceAsString(e));
            throw new PathHandlingException("error creating parent directory for " + m_endpoint.getPath(), e);
        }
    }

    enum DecodedStatus {

        OK(null),
        FAIL(null),
        FILE_NOT_FOUND("FileNotFoundException"),
        FILE_ALREADY_EXISTS("FileAlreadyExistsException");

        static final Pattern hdfsExceptionRE =
                Pattern.compile("\"exception\":\"(?<exception>(?:[^\"\\\\]|\\\\.)+)");

        static final Map<String, DecodedStatus> exceptions;

        static {
            ImmutableMap.Builder<String, DecodedStatus> builder = ImmutableMap.builder();
            for (DecodedStatus drsp: values()) {
                if (drsp.exception != null) {
                    builder.put(drsp.exception, drsp);
                }
            }
            exceptions = builder.build();
        }

        String exception;

        DecodedStatus(String exception) {
            this.exception = exception;
        }

        static DecodedStatus fromResponse(HttpResponse rsp) {
            if (rsp == null) return FAIL;
            switch (rsp.getStatusLine().getStatusCode()) {
            case HttpStatus.SC_OK:
            case HttpStatus.SC_CREATED:
            case HttpStatus.SC_ACCEPTED:
                return OK;
            case HttpStatus.SC_NOT_FOUND:
            case HttpStatus.SC_FORBIDDEN:
                DecodedStatus decoded = FAIL;
                String msg = "";
                try {
                    msg = EntityUtils.toString(rsp.getEntity(), Charsets.UTF_8);
                } catch (ParseException | IOException e) {
                    //m_logger.warn("could not load response body to parse error message", e);
                }
                Matcher mtc = hdfsExceptionRE.matcher(msg);
                if (mtc.find() && exceptions.containsKey(mtc.group("exception"))) {
                    decoded = exceptions.get(mtc.group("exception"));
                }
                return decoded;
            default:
                return FAIL;
            }
        }
    }

    @Immutable
    public static class HadoopRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context)  {
            boolean isRedirect=false;
            try {
                isRedirect = super.isRedirected(request, response, context);
            } catch (ProtocolException e) {
                e.printStackTrace();
            }
            if (!isRedirect) {
                int responseCode = response.getStatusLine().getStatusCode();
                if (responseCode == 301 || responseCode == 302 || responseCode == 307) {
                    isRedirect = true;
                }
            }
            return isRedirect;
        }

        @Override
        public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
            URI uri = getLocationURI(request, response, context);
            HttpUriRequest redirectRequest = null;
            String method = request.getRequestLine().getMethod();
            if (method.equalsIgnoreCase(HttpHead.METHOD_NAME)) {
                redirectRequest = new HttpHead(uri);
            } else if (method.equalsIgnoreCase(HttpPost.METHOD_NAME)) {
                HttpPost post = new HttpPost(uri);
                post.setEntity(((HttpEntityEnclosingRequest) request).getEntity());
                redirectRequest = post;
                redirectRequest.setHeader("Expect", "100-continue");
                if (post.getEntity() == null || post.getEntity().getContentLength() == 0) {
                    post.setHeader(OctetStreamContentTypeHeader);
                }
            } else if (method.equalsIgnoreCase(HttpGet.METHOD_NAME)) {
                redirectRequest = new HttpGet(uri);
            } else if (method.equalsIgnoreCase(HttpPut.METHOD_NAME)) {
                HttpPut put = new HttpPut(uri);
                put.setEntity(((HttpEntityEnclosingRequest) request).getEntity());
                redirectRequest = put;
                redirectRequest.setHeader("Expect", "100-continue");
                if (put.getEntity() == null || put.getEntity().getContentLength() == 0) {
                    put.setHeader(OctetStreamContentTypeHeader);
                }
            }
            return redirectRequest;
        }

    }

    static public class PathHandlingException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public PathHandlingException(String message, Throwable cause)
        {
            super(message, cause);
        }

        public PathHandlingException(Throwable cause)
        {
            super(cause);
        }

        public PathHandlingException(String message)
        {
            super(message);
        }
    }
}
