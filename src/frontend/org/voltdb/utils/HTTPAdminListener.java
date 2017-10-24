/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.entity.ContentType;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.AuthenticationResult;
import org.voltdb.ClientResponseImpl;
import org.voltdb.HTTPClientInterface;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.io.Resources;
import com.google_voltpatches.common.net.HostAndPort;
import javax.servlet.http.HttpServlet;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;

public class HTTPAdminListener {

    private static final VoltLogger m_log = new VoltLogger("HOST");
    public static final String REALM = "VoltDBRealm";

    // static resources
    public static final String RESOURCE_BASE = "dbmonitor";
    public static final String CSS_TARGET = "css";
    public static final String IMAGES_TARGET = "images";
    public static final String JS_TARGET = "js";

    // content types
    static final String JSON_CONTENT_TYPE = ContentType.APPLICATION_JSON.toString();
    static final String HTML_CONTENT_TYPE = "text/html;charset=utf-8";

    Server m_server;
    final static HTTPClientInterface httpClientInterface = new HTTPClientInterface();
    static boolean m_jsonEnabled;

    Map<String, String> m_htmlTemplates = new HashMap<>();
    final boolean m_mustListen;

    static String m_publicIntf;

    //Somewhat like Filter but we dont have Filter in version and jars we use.
    class VoltRequestHandler extends HttpServlet {
        VoltLogger logger = new VoltLogger("HOST");
        private String m_hostHeader = null;

        public VoltRequestHandler() {

        }

        @Override
        public void init() {

        }

        protected String getHostHeader() {
            if (m_hostHeader != null) {
                return m_hostHeader;
            }

            if (!m_publicIntf.isEmpty()) {
                m_hostHeader = m_publicIntf;
                return m_hostHeader;
            }

            InetAddress addr = null;
            int httpPort = VoltDB.DEFAULT_HTTP_PORT;
            try {
                String localMetadata = VoltDB.instance().getLocalMetadata();
                JSONObject jsObj = new JSONObject(localMetadata);
                JSONArray interfaces = jsObj.getJSONArray("interfaces");
                //The first interface is external interface if specified.
                String iface = interfaces.getString(0);
                addr = InetAddress.getByName(iface);
                httpPort = jsObj.getInt("httpPort");
            } catch (Exception e) {
                logger.warn("Failed to get HTTP interface information.", e);
            }
            if (addr == null) {
                addr = org.voltcore.utils.CoreUtils.getLocalAddress();
            }
            //Make the header string.
            m_hostHeader = addr.getHostAddress() + ":" + httpPort;
            return m_hostHeader;
        }

        public AuthenticationResult authenticate(HttpServletRequest request) {
            return httpClientInterface.authenticate(request);
        }

        @Override
        protected void doGet( HttpServletRequest request, HttpServletResponse response ) throws ServletException, IOException {
            response.setHeader("Host", getHostHeader());
        }

    }

        //Build a client response based json response
    private static String buildClientResponse(String jsonp, byte code, String msg) {
        ClientResponseImpl rimpl = new ClientResponseImpl(code, new VoltTable[0], msg);
        return HTTPClientInterface.asJsonp(jsonp, rimpl.toJSONString());
    }

    /*
     * Utility handler class to enable caching of static resources.
     * The static resources are package in jar file
     */
    class CacheStaticResourceHandler extends ResourceHandler {
        // target Directory location for folder w.r.t. resource base folder - dbmonitor
        public CacheStaticResourceHandler(final String target, int maxAge) {
            super();
            final String path = VoltDB.class.getResource(RESOURCE_BASE + File.separator + target).toExternalForm();
            if (m_log.isDebugEnabled()) {
                m_log.debug("Resource base path: " + path);
            }
            setResourceBase(path);
            // set etags along with cache age so that the http client's requests for fetching the
            // static resource is rate limited. Without cache age, client will requesting for
            // static more than needed
            setCacheControl("max-age=" + maxAge +", private");
            setEtags(true);
        }

        @SuppressWarnings("unused")
        private CacheStaticResourceHandler() {
            super();
            assert false : "Target location for static resource is needed to initialize the resource handler";
        }

        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
                           throws IOException, ServletException {
            super.handle(target, baseRequest, request, response);
            if (!baseRequest.isHandled() && m_log.isDebugEnabled()) {
                m_log.debug("Failed to process static resource: " + Paths.get(getResourceBase()));
            }
        }

    }

    /**
     * Load a template for the admin page, fill it out and return the value.
     * @param params The key-value set of variables to replace in the template.
     * @return The completed template.
     */
    String getHTMLForAdminPage(Map<String,String> params) {
        try {
            String template = m_htmlTemplates.get("admintemplate.html");
            for (Entry<String, String> e : params.entrySet()) {
                String key = e.getKey().toUpperCase();
                String value = e.getValue();
                if (key == null) continue;
                if (value == null) value = "NULL";
                template = template.replace("#" + key + "#", value);
            }
            return template;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return "<html><body>An unrecoverable error was encountered while generating this page.</body></html>";
    }

    private void loadTemplate(Class<?> clz, String name) throws Exception {
        URL url = Resources.getResource(clz, name);
        String contents = Resources.toString(url, Charsets.UTF_8);
        m_htmlTemplates.put(name, contents);
    }

    public HTTPAdminListener(
            boolean jsonEnabled, String intf, String publicIntf, int port,
            SslContextFactory sslContextFactory, boolean mustListen
            ) throws Exception {
        int poolsize = Integer.getInteger("HTTP_POOL_SIZE", 50);
        int timeout = Integer.getInteger("HTTP_REQUEST_TIMEOUT_SECONDS", 15);
        int cacheMaxAge = Integer.getInteger("HTTP_STATIC_CACHE_MAXAGE", 24*60*60); // 24 hours

        String resolvedIntf = intf == null ? "" : intf.trim().isEmpty() ? ""
                : HostAndPort.fromHost(intf).withDefaultPort(port).toString();

        m_publicIntf = publicIntf == null ? resolvedIntf : publicIntf.trim().isEmpty() ? resolvedIntf
                : HostAndPort.fromHost(publicIntf).withDefaultPort(port).toString();

        /*
         * Don't force us to look at a huge pile of threads
         */
        final QueuedThreadPool qtp = new QueuedThreadPool(
                poolsize,
                1, // minimum threads
                timeout * 1000,
                new LinkedBlockingQueue<>(poolsize + 16)
                );

        m_server = new Server(qtp);
        m_server.setAttribute(
                "org.eclipse.jetty.server.Request.maxFormContentSize",
                new Integer(HTTPClientInterface.MAX_QUERY_PARAM_SIZE)
                );

        m_mustListen = mustListen;
        // PRE-LOAD ALL HTML TEMPLATES (one for now)
        try {
            loadTemplate(HTTPAdminListener.class, "admintemplate.html");
        }
        catch (Exception e) {
            VoltLogger logger = new VoltLogger("HOST");
            logger.error("Unable to load HTML templates from jar for admin pages.", e);
            throw e;
        }

        // NOW START SocketConnector and create Jetty server but dont start.
        ServerConnector connector = null;
        try {
            if (sslContextFactory == null) { // basic HTTP
                // The socket channel connector seems to be faster for our use
                //SelectChannelConnector connector = new SelectChannelConnector();
                connector = new ServerConnector(m_server);

                if (intf != null && !intf.trim().isEmpty()) {
                    connector.setHost(intf);
                }
                connector.setPort(port);
                connector.setName("VoltDB-HTTPD");
                //open the connector here so we know if port is available and Init work can retry with next port.
                connector.open();
                m_server.addConnector(connector);
            } else { // HTTPS
                m_server.addConnector(getSSLServerConnector(sslContextFactory, intf, port));
            }
            ContextHandlerCollection handlers = new ContextHandlerCollection();

            ServletContextHandler rootContext = new ServletContextHandler(handlers, "/",
                    ServletContextHandler.SESSIONS);
            rootContext.addServlet(DBMonitorServlet.class, "/");

            ServletContextHandler apiContext = new ServletContextHandler(handlers, "/api/1.0",
                    ServletContextHandler.SESSIONS);
            apiContext.addServlet(ApiRequestServlet.class, "/");

            ServletContextHandler catalogContext = new ServletContextHandler(handlers, "/catalog",
                    ServletContextHandler.SESSIONS);
            catalogContext.addServlet(CatalogRequestServlet.class, "/");

            ServletContextHandler depContext = new ServletContextHandler(handlers, "/deployment",
                    ServletContextHandler.SESSIONS);
            depContext.addServlet(DeploymentRequestServlet.class, "/");
            ServletContextHandler profileContext = new ServletContextHandler(handlers, "/profile",
                    ServletContextHandler.SESSIONS);
            profileContext.addServlet(UserProfileServlet.class, "/");


            //"/"
//            ContextHandler dbMonitorHandler = new ContextHandler("/");
//            dbMonitorHandler.setHandler(new DBMonitorHandler());
//
//            ///api/1.0/
//            ContextHandler apiRequestHandler = new ContextHandler("/api/1.0");
//            // the default is 200k which well short of out 2M row size limit
//            apiRequestHandler.setMaxFormContentSize(HTTPClientInterface.MAX_QUERY_PARAM_SIZE);
//            // close another attack vector where potentially one may send a large number of keys
//            apiRequestHandler.setMaxFormKeys(HTTPClientInterface.MAX_FORM_KEYS);
//            apiRequestHandler.setHandler(new APIRequestHandler());
//
//            ///catalog
//            ContextHandler catalogRequestHandler = new ContextHandler("/catalog");
//            catalogRequestHandler.setHandler(new CatalogRequestHandler());
//
//            ///deployment
//            ContextHandler deploymentRequestHandler = new ContextHandler("/deployment");
//            m_deploymentHandler = new DeploymentRequestHandler();
//            deploymentRequestHandler.setHandler(m_deploymentHandler);
//            deploymentRequestHandler.setAllowNullPathInfo(true);
//
//            ///profile
//            ContextHandler profileRequestHandler = new ContextHandler("/profile");
//            profileRequestHandler.setHandler(new UserProfileHandler());
//
            ContextHandler cssResourceHandler = new ContextHandler("/css");
            ResourceHandler cssResource = new CacheStaticResourceHandler(CSS_TARGET, cacheMaxAge);
            cssResourceHandler.setHandler(cssResource);

            ContextHandler imageResourceHandler = new ContextHandler("/images");
            ResourceHandler imagesResource = new CacheStaticResourceHandler(IMAGES_TARGET, cacheMaxAge);
            imageResourceHandler.setHandler(imagesResource);

            ContextHandler jsResourceHandler = new ContextHandler("/js");
            ResourceHandler jsResource = new CacheStaticResourceHandler(JS_TARGET, cacheMaxAge);
            jsResourceHandler.setHandler(jsResource);


            handlers.addHandler(cssResourceHandler);
            handlers.addHandler(imageResourceHandler);
            handlers.addHandler(jsResourceHandler);

            GzipHandler compressResourcesHandler = new GzipHandler();
            compressResourcesHandler.setHandler(handlers);

            compressResourcesHandler.addExcludedMimeTypes(JSON_CONTENT_TYPE);
            compressResourcesHandler.setIncludedMimeTypes("application/x-javascript", "text/css" ,
                    "image/gif", "image/png", "image/jpeg", HTML_CONTENT_TYPE);

            m_server.setHandler(compressResourcesHandler);

            httpClientInterface.setTimeout(timeout);
            m_jsonEnabled = jsonEnabled;
        } catch (Exception e) {
            // double try to make sure the port doesn't get eaten
            try { connector.close(); } catch (Exception e2) {}
            try { m_server.destroy(); } catch (Exception e2) {}
            throw new Exception(e);
        }
    }

    private ServerConnector getSSLServerConnector(SslContextFactory sslContextFactory, String intf, int port)
        throws IOException {
        // SSL HTTP Configuration
        HttpConfiguration httpsConfig = new HttpConfiguration();
        httpsConfig.setSecureScheme("ssl");
        httpsConfig.setSecurePort(port);
        //Add this customizer to indicate we are in ssl land
        httpsConfig.addCustomizer(new SecureRequestCustomizer());
        HttpConnectionFactory factory = new HttpConnectionFactory(httpsConfig);

        // SSL Connector
        ServerConnector connector = new ServerConnector(m_server,
            new SslConnectionFactory(sslContextFactory,HttpVersion.HTTP_1_1.asString()),
            factory);
        if (intf != null && !intf.trim().isEmpty()) {
            connector.setHost(intf);
        }
        connector.setPort(port);
        connector.setName("VoltDB-HTTPS");
        connector.open();

        return connector;
    }

    public void start() throws Exception {
        try {
            m_server.start();
        } catch (Exception e) {
            // double try to make sure the port doesn't get eaten
            try { m_server.stop(); } catch (Exception e2) {}
            try { m_server.destroy(); } catch (Exception e2) {}
            //We only throw exception to halt and we expect to mustListen;
            if (m_mustListen) {
                throw new Exception(e);
            }
        }
    }

    public void stop() {
        if (httpClientInterface != null) {
            httpClientInterface.stop();
        }

        try {
            m_server.stop();
            m_server.join();
        }
        catch (Exception e) {}
        try { m_server.destroy(); } catch (Exception e2) {}
        m_server = null;
    }

    public void notifyOfCatalogUpdate() {
        if (httpClientInterface != null) {
            httpClientInterface.notifyOfCatalogUpdate();
        }
    }
}
