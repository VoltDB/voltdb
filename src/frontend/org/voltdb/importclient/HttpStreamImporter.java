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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.annotation.Immutable;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.importer.CSVInvocation;
import org.voltdb.importer.ImportHandlerProxy;

/**
 * Implement a BundleActivator interface and extend ImportHandlerProxy.
 */
public class HttpStreamImporter extends ImportHandlerProxy implements BundleActivator {
    private final static Pattern PERCENT = Pattern.compile("%");

    public static final Header OctetStreamContentTypeHeader =
            new BasicHeader("Content-Type", ContentType.APPLICATION_OCTET_STREAM.getMimeType());

    private Properties m_properties;
    private ServerSocket m_serverSocket;
    private String m_procedure;
    private final ArrayList<ClientConnectionHandler> m_clients = new ArrayList<ClientConnectionHandler>();

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
            for (ClientConnectionHandler s : m_clients) {
                s.stopClient();
            }
            m_clients.clear();
            m_serverSocket.close();
            m_serverSocket = null;
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
        return "SocketImporter";
    }

    /**
     * This is called with the properties that are supplied in the deployment.xml
     * Do any initialization here.
     * @param p
     */
    @Override
    public void configure(Properties p) {
        m_properties = (Properties) p.clone();
        String s = (String )m_properties.get("port");
        m_procedure = (String )m_properties.get("procedure");
        if (m_procedure == null || m_procedure.trim().length() == 0) {
            throw new RuntimeException("Missing procedure.");
        }
        try {
            if (m_serverSocket != null) {
                m_serverSocket.close();
            }
            m_serverSocket = new ServerSocket(Integer.parseInt(s));
        } catch (IOException ex) {
           ex.printStackTrace();
           throw new RuntimeException(ex.getCause());
        }
    }

    //This is ClientConnection handler to read and dispatch data to stored procedure.
    private class ClientConnectionHandler extends Thread {
        private final Socket m_clientSocket;
        private final String m_procedure;
        private final ImportHandlerProxy m_importHandlerProxy;

        public ClientConnectionHandler(ImportHandlerProxy ic, Socket clientSocket, String procedure) {
            m_importHandlerProxy = ic;
            m_clientSocket = clientSocket;
            m_procedure = procedure;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(m_clientSocket.getInputStream()));
                    while (true) {
                        String line = in.readLine();
                        //You should convert your data to params here.
                        if (line == null) break;
                        CSVInvocation invocation = new CSVInvocation(m_procedure, line);
                        if (!callProcedure(m_importHandlerProxy, invocation)) {
                            System.out.println("Inserted failed: " + line);
                        }
                    }
                    m_clientSocket.close();
                    System.out.println("Client Closed.");
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }

        public void stopClient() {
            try {
                m_clientSocket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * This is called when server is ready to accept any transactions.
     */
    @Override
    public void readyForData() {
        try {
            info("Configured and ready with properties: " + m_properties);
            String procedure = m_properties.getProperty("procedure");
            while (true) {
                Socket clientSocket = m_serverSocket.accept();
                ClientConnectionHandler ch = new ClientConnectionHandler(this, clientSocket, procedure);
                m_clients.add(ch);
                ch.start();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Convenient method to check if the given URI string is a WebHDFS URL.
     * See {@link #isHdfsUri(java.net.URI)} for more.
     */
    public static boolean isHdfsUri(String uriStr)
    {
        return isHdfsUri(URI.create(PERCENT.matcher(uriStr).replaceAll("")));
    }

    /**
     * Checks if the given URI is a WebHDFS URL. WebHDFS URLs have the form
     * http[s]://hostname:port/webhdfs/v1/.
     *
     * @param endpoint    The URI
     * @return true if it is a WebHDFS URL, false otherwise.
     */
    public static boolean isHdfsUri(URI endpoint)
    {
        final String path = endpoint.getPath();
        if (path != null && path.indexOf('/', 1) != -1) {
            return path.substring(1, path.indexOf('/', 1)).equalsIgnoreCase("webhdfs");
        } else {
            return false;
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
                if (isHdfsUri(uri)) {
                    redirectRequest.setHeader("Expect", "100-continue");
                }
                if (post.getEntity() == null || post.getEntity().getContentLength() == 0) {
                    post.setHeader(OctetStreamContentTypeHeader);
                }
            } else if (method.equalsIgnoreCase(HttpGet.METHOD_NAME)) {
                redirectRequest = new HttpGet(uri);
            } else if (method.equalsIgnoreCase(HttpPut.METHOD_NAME)) {
                HttpPut put = new HttpPut(uri);
                put.setEntity(((HttpEntityEnclosingRequest) request).getEntity());
                redirectRequest = put;
                if (isHdfsUri(uri)) {
                    redirectRequest.setHeader("Expect", "100-continue");
                }
                if (put.getEntity() == null || put.getEntity().getContentLength() == 0) {
                    put.setHeader(OctetStreamContentTypeHeader);
                }
            }
            return redirectRequest;
        }

    }

}
