/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.voltcore.logging.VoltLogger;
import org.voltdb.HTTPClientInterface;
import org.voltdb.VoltDB;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.io.Resources;
import com.google_voltpatches.common.net.HostAndPort;
import java.util.HashSet;
import java.util.Set;
import javax.servlet.SessionTrackingMode;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.server.session.DefaultSessionIdManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;

public class HTTPAdminListener {

    private static final VoltLogger m_log = new VoltLogger("HOST");

    // static resources
    public static final String RESOURCE_BASE = "dbmonitor";
    public static final String CSS_TARGET = "css";
    public static final String IMAGES_TARGET = "images";
    public static final String JS_TARGET = "js";

    // content types
    static final String JSON_CONTENT_TYPE = ContentType.APPLICATION_JSON.toString();
    static final String HTML_CONTENT_TYPE = "text/html;charset=utf-8";

    final Server m_server;
    final DefaultSessionIdManager m_idmanager;
    final SessionHandler m_sessionHandler = new SessionHandler();

    final HTTPClientInterface httpClientInterface = new HTTPClientInterface();
    boolean m_jsonEnabled;

    Map<String, String> m_htmlTemplates = new HashMap<>();
    final boolean m_mustListen;

    String m_publicIntf;


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
        final HTTPQueuedThreadPool qtp = new HTTPQueuedThreadPool(
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

        // Inactivity timeout defaults to 30 secs, but can be overridden by environment variable
        m_sessionHandler.setMaxInactiveInterval(HTTPClientInterface.MAX_SESSION_INACTIVITY_SECONDS);
        m_idmanager = new HttpSessionIdManager(m_server);
        m_server.setSessionIdManager(m_idmanager);
        m_idmanager.setWorkerName("vmc");
        m_sessionHandler.setSessionIdManager(m_idmanager);
        m_sessionHandler.setServer(m_server);

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
        boolean useSecure = false;
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
                useSecure = true;
                m_server.addConnector(getSSLServerConnector(sslContextFactory, intf, port));
            }

            ServletContextHandler rootContext = new ServletContextHandler(ServletContextHandler.SESSIONS);

            ServletHandler servlets = rootContext.getServletHandler();
            // make the JSESSIONID context-specific by adding port number
            rootContext.getSessionHandler().getSessionCookieConfig().setName("JSESSIONID_" + Integer.toString(port));
            // the default is 200k which well short of out 2M row size limit
            rootContext.setMaxFormContentSize(HTTPClientInterface.MAX_QUERY_PARAM_SIZE);
            // close another attack vector where potentially one may send a large number of keys
            rootContext.setMaxFormKeys(HTTPClientInterface.MAX_FORM_KEYS);
            rootContext.getSessionHandler().getSessionCookieConfig().setHttpOnly(true);
            //Only use cookie mode and dont support URL
            Set<SessionTrackingMode> trackModes = new HashSet<>();
            trackModes.add(SessionTrackingMode.COOKIE);
            rootContext.getSessionHandler().setSessionTrackingModes(trackModes);
            if (useSecure) {
                //Make cookie secure when using SSL
                rootContext.getSessionHandler().getSessionCookieConfig().setSecure(useSecure);
            }

            ContextHandler cssResourceHandler = new ContextHandler("/css");
            ResourceHandler cssResource = new CacheStaticResourceHandler(CSS_TARGET, cacheMaxAge);
            cssResource.setDirectoriesListed(false);
            cssResourceHandler.setHandler(cssResource);

            ContextHandler imageResourceHandler = new ContextHandler("/images");
            ResourceHandler imagesResource = new CacheStaticResourceHandler(IMAGES_TARGET, cacheMaxAge);
            imagesResource.setDirectoriesListed(false);
            imageResourceHandler.setHandler(imagesResource);

            ContextHandler jsResourceHandler = new ContextHandler("/js");
            ResourceHandler jsResource = new CacheStaticResourceHandler(JS_TARGET, cacheMaxAge);
            jsResource.setDirectoriesListed(false);
            jsResourceHandler.setHandler(jsResource);

            //Add all to a collection which will be wrapped by GzipHandler we set GzipHandler to the server.
            ContextHandlerCollection handlers = new ContextHandlerCollection();
            handlers.addHandler(disableTraceMethodForHandler(rootContext));
            handlers.addHandler(disableTraceMethodForHandler(cssResourceHandler));
            handlers.addHandler(disableTraceMethodForHandler(imageResourceHandler));
            handlers.addHandler(disableTraceMethodForHandler(jsResourceHandler));

            GzipHandler compressResourcesHandler = new GzipHandler();
            compressResourcesHandler.setHandler(handlers);
            compressResourcesHandler.addExcludedMimeTypes(JSON_CONTENT_TYPE);
            compressResourcesHandler.setIncludedMimeTypes("application/x-javascript", "text/css" ,
                    "image/gif", "image/png", "image/jpeg", HTML_CONTENT_TYPE);

            compressResourcesHandler.setServer(m_server);
            m_server.setHandler(compressResourcesHandler);

            //Following are the servelets jetty is configured with see URL pattern for what they handle.
            servlets.addServletWithMapping(DBMonitorServlet.class, "/").setAsyncSupported(true);
            servlets.addServletWithMapping(ApiRequestServlet.class, "/api/1.0/*").setAsyncSupported(true);
            servlets.addServletWithMapping(ApiRequestServletV2.class, "/api/2.0/*").setAsyncSupported(true);
            servlets.addServletWithMapping(CatalogRequestServlet.class, "/catalog/*").setAsyncSupported(true);
            servlets.addServletWithMapping(DeploymentRequestServlet.class, "/deployment/*").setAsyncSupported(true);
            servlets.addServletWithMapping(UserProfileServlet.class, "/profile/*").setAsyncSupported(true);
            servlets.addServletWithMapping(LogoutServlet.class, "/logout/*").setAsyncSupported(true);

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
        System.setProperty("org.eclipse.jetty.server.LEVEL", "ERROR");
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
    }

    //Clean all active sessions. This is called when UAC happens. If UAC has changed users/password
    //information we need to make users re-login. We could add more smart during UAC that if no user info is modified don't do this.
    //or only clean up the sessions with updated users' credentials.
    public void notifyOfCatalogUpdate() {
        try {
            ((HttpSessionIdManager)m_idmanager).cleanSessions();
        } catch (Exception ex) {
            m_log.error("Failed to update HTTP interface after catalog update", ex);
        }
    }

    public void dontStoreAuthenticationResultInHttpSession() {
        if (httpClientInterface != null) {
            httpClientInterface.dontStoreAuthenticationResultInHttpSession();
        }
    }

    private static class HttpSessionIdManager extends DefaultSessionIdManager {

        HttpSessionIdManager(Server server) {
            super(server);
        }

        //remove all sessions in cache
        void cleanSessions() throws Exception {
           for (SessionHandler sh:getSessionHandlers()) {
               sh.getSessionCache().shutdown();
            }
        }
    }

    private Handler disableTraceMethodForHandler(Handler h){

        Constraint disableTraceConstraint = new Constraint();
        disableTraceConstraint.setName("Disable TRACE");
        disableTraceConstraint.setAuthenticate(true);

        ConstraintMapping mapping = new ConstraintMapping();
        mapping.setConstraint(disableTraceConstraint);
        mapping.setMethod("TRACE");
        mapping.setPathSpec("/");

        Constraint omissionConstraint = new Constraint();
        ConstraintMapping omissionMapping = new ConstraintMapping();
        omissionMapping.setConstraint(omissionConstraint);
        omissionMapping.setMethod("*");
        omissionMapping.setPathSpec("/");


        ConstraintSecurityHandler handler = new ConstraintSecurityHandler();
        handler.addConstraintMapping(mapping);
        handler.addConstraintMapping(omissionMapping);
        handler.setHandler(h);
        return handler;
    }
}
