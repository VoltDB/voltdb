/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.annotation.XmlAttribute;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.schema.JsonSchema;
import org.eclipse.jetty.continuation.Continuation;
import org.eclipse.jetty.continuation.ContinuationSupport;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.AuthenticationResult;
import org.voltdb.ClientResponseImpl;
import org.voltdb.HTTPClientInterface;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.common.Permission;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.ExportType;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.compiler.deploymentfile.UsersType.User;
import org.voltdb.compilereport.ReportMaker;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.io.Resources;

public class HTTPAdminListener {

    private static final VoltLogger m_log = new VoltLogger("HOST");
    public static final String REALM = "VoltDBRealm";

    Server m_server;
    HTTPClientInterface httpClientInterface = new HTTPClientInterface();
    final boolean m_jsonEnabled;

    Map<String, String> m_htmlTemplates = new HashMap<String, String>();
    final boolean m_mustListen;
    final DeploymentRequestHandler m_deploymentHandler;

    //Somewhat like Filter but we dont have Filter in version and jars we use.
    class VoltRequestHandler extends AbstractHandler {
        VoltLogger logger = new VoltLogger("HOST");
        private String m_hostHeader = null;

        protected String getHostHeader() {
            if (m_hostHeader != null) {
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

        public AuthenticationResult authenticate(Request request) {
            return httpClientInterface.authenticate(request);
        }

        @Override
        public void handle(String string, Request rqst, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            response.setHeader("Host", getHostHeader());
        }

        //Build a client response based json response
        protected String buildClientResponse(String jsonp, byte code, String msg) {
            ClientResponseImpl rimpl = new ClientResponseImpl(code, new VoltTable[0], msg);
            if (jsonp != null) {
                return String.format("%s( %s )", jsonp, rimpl.toJSONString());
            }
            return rimpl.toJSONString();
        }

    }

    class DBMonitorHandler extends VoltRequestHandler {

        @Override
        public void handle(String target, Request baseRequest,
                           HttpServletRequest request, HttpServletResponse response)
                            throws IOException, ServletException {
            super.handle(target, baseRequest, request, response);
            try{

                // if this is an internal jetty retry, then just tell
                // jetty we're still working on it. There is a risk of
                // masking other errors in doing this, but it's probably
                // low compared with the default policy of retrys.
                Continuation cont = ContinuationSupport.getContinuation(baseRequest);
                // this is set to false on internal jetty retrys
                if (!cont.isInitial()) {
                    // The continuation object has been woken up by the
                    // retry. Tell it to go back to sleep.
                    cont.suspend();
                    return;
                }

                //Special handling for API as they continue and setHandled differently
                if (baseRequest.getRequestURI().contains("/api/1.0/")) {
                    baseRequest.setHandled(false);
                    return;
                }
                //Send old /studio back to "/"
                if (baseRequest.getRequestURI().contains("/studio")) {
                    response.sendRedirect("/");
                    baseRequest.setHandled(true);
                    return;
                }
                // redirect the base dir
                if (target.equals("/")) target = "/index.htm";
                // check if a file exists
                URL url = VoltDB.class.getResource("dbmonitor" + target);
                if (url == null) {
                    // write 404
                    String msg = "404: Resource not found.\n";
                    response.setContentType("text/plain;charset=utf-8");
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    baseRequest.setHandled(true);
                    response.getWriter().print(msg);
                    return;
                }

                // read the template
                InputStream is = VoltDB.class.getResourceAsStream("dbmonitor" + target);

                if (target.endsWith("/index.htm")) {

                    // set the headers
                    response.setContentType("text/html;charset=utf-8");
                    response.setStatus(HttpServletResponse.SC_OK);
                    baseRequest.setHandled(true);

                    OutputStream os = response.getOutputStream();
                    BufferedInputStream bis = new BufferedInputStream(is);

                    int c = -1;
                    while ((c = bis.read()) != -1) {
                        os.write(c);
                    }
                }
                else {
                    // set the mime type in a giant hack
                    String mime = "text/html;charset=utf-8";
                    if (target.endsWith(".js"))
                        mime = "application/x-javascript;charset=utf-8";
                    if (target.endsWith(".css"))
                        mime = "text/css;charset=utf-8";
                    if (target.endsWith(".gif"))
                        mime = "image/gif";
                    if (target.endsWith(".png"))
                        mime = "image/png";
                    if ((target.endsWith(".jpg")) || (target.endsWith(".jpeg")))
                        mime = "image/jpeg";

                    // set the headers
                    response.setContentType(mime);
                    response.setStatus(HttpServletResponse.SC_OK);
                    baseRequest.setHandled(true);

                    // write the file out
                    BufferedInputStream bis = new BufferedInputStream(is);
                    OutputStream os = response.getOutputStream();
                    int c = -1;
                    while ((c = bis.read()) != -1) {
                        os.write(c);
                    }
                }
            }catch(Exception ex){
                logger.info("Not servicing url: " + baseRequest.getRequestURI() + " Details: "+ ex.getMessage());
            }
        }
    }

    class CatalogRequestHandler extends VoltRequestHandler {

        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
                           throws IOException, ServletException {

            super.handle(target, baseRequest, request, response);
            handleReportPage(baseRequest, response);
        }

    }

    class DDLRequestHandler extends VoltRequestHandler {

        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
                           throws IOException, ServletException {

            super.handle(target, baseRequest, request, response);
            byte[] reportbytes = VoltDB.instance().getCatalogContext().getFileInJar("autogen-ddl.sql");
            String ddl = new String(reportbytes, Charsets.UTF_8);
            response.setContentType("text/plain;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            response.getWriter().print(ddl);
        }

    }

    //This is a wrapper to generate JSON for profile of authenticated user.
    private final class Profile {
        private final String user;
        private final String permissions[];
        public Profile(String u, String[] p) {
            user = u;
            permissions = p;
        }
        public String getUser() {
            return user;
        }
        public String[] getPermissions() {
            return permissions;
        }
    }

    // /profile handler
    class UserProfileHandler extends VoltRequestHandler {
        private final ObjectMapper m_mapper;

        public UserProfileHandler() {
            m_mapper = new ObjectMapper();
            //We want jackson to stop closing streams
            m_mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        }

        // GET on /profile resources.
        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
                           throws IOException, ServletException {
            //jsonp is specified when response is expected to go to javascript function.
            String jsonp = request.getParameter("jsonp");
            AuthenticationResult authResult = null;
            try {
                response.setContentType("application/json;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                authResult = authenticate(baseRequest);
                if (!authResult.isAuthenticated()) {
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, authResult.m_message));
                } else {
                    if (jsonp != null) {
                        response.getWriter().write(jsonp + "(");
                    }
                    m_mapper.writeValue(response.getWriter(), new Profile(authResult.m_user, authResult.m_perms));
                    if (jsonp != null) {
                        response.getWriter().write(")");
                    }
                }
                baseRequest.setHandled(true);
            } catch (Exception ex) {
              logger.info("Not servicing url: " + baseRequest.getRequestURI() + " Details: "+ ex.getMessage(), ex);
            } finally {
                httpClientInterface.releaseClient(authResult, false);
            }
        }
    }

    //This is for password on User in the deployment to not to be reported.
    abstract class IgnorePasswordMixIn {
        @JsonIgnore abstract String getPassword();
    }
    abstract class IgnoreLegacyExportAttributesMixIn {
        @JsonIgnore abstract String getExportconnectorclass();
        @JsonIgnore abstract ServerExportEnum getTarget();
        @JsonIgnore abstract Boolean isEnabled();
    }

    @JsonPropertyOrder({"name","roles","password","plaintext","id"})
    public class IdUser extends UsersType.User {
        @XmlAttribute(name = "id")
        protected String id;

        IdUser(UsersType.User user, String header) {
            this.name = user.getName();
            this.roles = user.getRoles();
            this.password = user.getPassword();
            this.plaintext = user.isPlaintext();
            this.id = header + "/deployment/users/" + this.name;
        }

        @JsonProperty("id")
        public void setId(String value) {
            this.id = value;
        }

        @JsonProperty("id")
        public String getId() {
            return id;
        }
    }

    class DeploymentRequestHandler extends VoltRequestHandler {

        final ObjectMapper m_mapper;
        String m_schema = "";

        public DeploymentRequestHandler() {
            m_mapper = new ObjectMapper();
            //Mixin for to not output passwords.
            m_mapper.getSerializationConfig().addMixInAnnotations(UsersType.User.class, IgnorePasswordMixIn.class);
            m_mapper.getSerializationConfig().addMixInAnnotations(ExportType.class, IgnoreLegacyExportAttributesMixIn.class);
            //We want jackson to stop closing streams
            m_mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
            try {
                JsonSchema schema = m_mapper.generateJsonSchema(DeploymentType.class);
                m_schema = schema.toString();
            } catch (JsonMappingException ex) {
                m_log.warn("Failed to generate JSON schema: ", ex);
            }
        }

        //Get deployment from catalog context
        private DeploymentType getDeployment() {
            return VoltDB.instance().getCatalogContext().getDeployment();
        }

        //Get deployment bytes from catalog context
        private byte[] getDeploymentBytes() {
            return VoltDB.instance().getCatalogContext().getDeploymentBytes();
        }

        private UsersType.User findUser(String user, DeploymentType dep) {
            if (dep.getUsers() != null) {
                for(User u : dep.getUsers().getUser()) {
                    if (user.equalsIgnoreCase(u.getName())) {
                        return u;
                    }
                }
            }
            return null;
        }

        // TODO - subresources.
        // We support
        // /deployment/cluster
        // /deployment/paths
        // /deployment/partitionDetection
        // /deployment/adminMode
        // /deployment/heartbeat
        // /deployment/httpd
        // /deployment/replication
        // /deployment/snapshot
        // /deployment/export
        // /deployment/users
        // /deployment/commandlog
        // /deployment/systemsettings
        // /deployment/security
        @Override
        public void handle(String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response)
                           throws IOException, ServletException {

            super.handle(target, baseRequest, request, response);

            //jsonp is specified when response is expected to go to javascript function.
            String jsonp = request.getParameter("jsonp");
            AuthenticationResult authResult = null;
            try {
                response.setContentType("application/json;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);

                //Requests require authentication.
                authResult = authenticate(baseRequest);
                if (!authResult.isAuthenticated()) {
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, authResult.m_message));
                    baseRequest.setHandled(true);
                    return;
                }
                //Authenticated but has no permissions.
                if (!authResult.m_authUser.hasPermission(Permission.ADMIN)) {
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Permission denied"));
                    baseRequest.setHandled(true);
                    return;
                }

                //Authenticated and has ADMIN permission
                if (baseRequest.getRequestURI().contains("/download")) {
                    //Deployment xml is text/xml
                    response.setContentType("text/xml;charset=utf-8");
                    response.getWriter().write(new String(getDeploymentBytes()));
                } else if (baseRequest.getRequestURI().contains("/users")) {
                    if (request.getMethod().equalsIgnoreCase("POST")) {
                        handleUpdateUser(jsonp, target, baseRequest, request, response, authResult.m_client);
                    } else if (request.getMethod().equalsIgnoreCase("PUT")) {
                        handleCreateUser(jsonp, target, baseRequest, request, response, authResult.m_client);
                    } else if (request.getMethod().equalsIgnoreCase("DELETE")) {
                        handleRemoveUser(jsonp, target, baseRequest, request, response, authResult.m_client);
                    } else {
                        handleGetUsers(jsonp, target, baseRequest, request, response, authResult.m_client);
                    }
                } else if (baseRequest.getRequestURI().contains("/export/type")) {
                    handleGetExportTypes(jsonp, response);
                } else {
                    if (request.getMethod().equalsIgnoreCase("POST")) {
                        handleUpdateDeployment(jsonp, target, baseRequest, request, response, authResult.m_client);
                    } else {
                        //non POST
                        if (jsonp != null) {
                            response.getWriter().write(jsonp + "(");
                        }
                        m_mapper.writeValue(response.getWriter(), getDeployment());
                        if (jsonp != null) {
                            response.getWriter().write(")");
                        }
                    }
                }
                baseRequest.setHandled(true);
            } catch (Exception ex) {
              logger.info("Not servicing url: " + baseRequest.getRequestURI() + " Details: "+ ex.getMessage(), ex);
            } finally {
                httpClientInterface.releaseClient(authResult, false);
            }
        }

        //Update the deployment
        public void handleUpdateDeployment(String jsonp, String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response, Client client)
                           throws IOException, ServletException {
            String deployment = request.getParameter("deployment");
            if (deployment == null || deployment.length() == 0) {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to get deployment information."));
                return;
            }
            try {
                DeploymentType newDeployment = m_mapper.readValue(deployment, DeploymentType.class);
                if (newDeployment == null) {
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to parse deployment information."));
                    return;
                }

                DeploymentType currentDeployment = this.getDeployment();
                if (currentDeployment.getUsers() != null) {
                    newDeployment.setUsers(currentDeployment.getUsers());
                }

                String dep = CatalogUtil.getDeployment(newDeployment);
                if (dep == null || dep.trim().length() <= 0) {
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to build deployment information."));
                    return;
                }
                Object[] params = new Object[] { null, dep};
                //Call sync as nothing else can happen when this is going on.
                client.callProcedure("@UpdateApplicationCatalog", params);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.SUCCESS, "Deployment Updated."));
            } catch (Exception ex) {
                logger.error("Failed to update deployment from API", ex);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, ex.toString()));
            }
        }

        //Handle POST for users
        public void handleUpdateUser(String jsonp, String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response, Client client)
                           throws IOException, ServletException {
            String update = request.getParameter("user");
            if (update == null || update.trim().length() == 0) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to get user information."));
                return;
            }
            try {
                User newUser = m_mapper.readValue(update, User.class);
                if (newUser == null) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to parse user information."));
                    return;
                }

                DeploymentType newDeployment = CatalogUtil.getDeployment(new ByteArrayInputStream(getDeploymentBytes()));
                User user = null;
                String[] splitTarget = target.split("/");
                if (splitTarget.length == 3) {
                    user = findUser(splitTarget[2], newDeployment);
                }
                if (user == null) {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "User not found"));
                    return;
                }
                user.setName(newUser.getName());
                user.setPassword(newUser.getPassword());
                user.setPlaintext(newUser.isPlaintext());
                user.setRoles(newUser.getRoles());

                String dep = CatalogUtil.getDeployment(newDeployment);
                if (dep == null || dep.trim().length() <= 0) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to build deployment information."));
                    return;
                }
                Object[] params = new Object[] { null, dep};
                //Call sync as nothing else can happen when this is going on.
                client.callProcedure("@UpdateApplicationCatalog", params);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.SUCCESS, "User Updated."));
                notifyOfCatalogUpdate();
            } catch (Exception ex) {
                logger.error("Failed to update user from API", ex);
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, ex.toString()));
            }
        }

        //Handle PUT for users
        public void handleCreateUser(String jsonp, String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response, Client client)
                           throws IOException, ServletException {
            String update = request.getParameter("user");
            if (update == null || update.trim().length() == 0) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to get user information."));
                return;
            }
            try {
                User newUser = m_mapper.readValue(update, User.class);
                if (newUser == null) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to parse user information."));
                    return;
                }

                DeploymentType newDeployment = CatalogUtil.getDeployment(new ByteArrayInputStream(getDeploymentBytes()));
                User user = null;
                String[] splitTarget = target.split("/");
                if (splitTarget.length == 3) {
                    user = findUser(splitTarget[2], newDeployment);
                } else {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "User not found"));
                    return;
                }
                String returnString = "User created";
                if (user == null) {
                    if (newDeployment.getUsers() == null) {
                        newDeployment.setUsers(new UsersType());
                    }
                    newDeployment.getUsers().getUser().add(newUser);
                    response.setStatus(HttpServletResponse.SC_CREATED);
                } else {
                    user.setName(newUser.getName());
                    user.setPassword(newUser.getPassword());
                    user.setPlaintext(newUser.isPlaintext());
                    user.setRoles(newUser.getRoles());
                    returnString = "User updated";
                    response.setStatus(HttpServletResponse.SC_OK);
                }

                String dep = CatalogUtil.getDeployment(newDeployment);
                if (dep == null || dep.trim().length() <= 0) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to build deployment information."));
                    return;
                }
                Object[] params = new Object[] { null, dep};
                //Call sync as nothing else can happen when this is going on.
                client.callProcedure("@UpdateApplicationCatalog", params);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.SUCCESS, returnString));
                notifyOfCatalogUpdate();
            } catch (Exception ex) {
                logger.error("Failed to create user from API", ex);
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, ex.toString()));
            }
        }

        //Handle DELETE for users
        public void handleRemoveUser(String jsonp, String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response, Client client)
                           throws IOException, ServletException {
            try {
                DeploymentType newDeployment = CatalogUtil.getDeployment(new ByteArrayInputStream(getDeploymentBytes()));
                User user = null;
                String[] splitTarget = target.split("/");
                if (splitTarget.length == 3) {
                    user = findUser(splitTarget[2], newDeployment);
                }
                if (user == null) {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "User not found"));
                    return;
                }
                if (newDeployment.getUsers().getUser().size() == 1) {
                    newDeployment.setUsers(null);
                } else {
                    newDeployment.getUsers().getUser().remove(user);
                }

                String dep = CatalogUtil.getDeployment(newDeployment);
                if (dep == null || dep.trim().length() <= 0) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to build deployment information."));
                    return;
                }
                Object[] params = new Object[] { null, dep};
                //Call sync as nothing else can happen when this is going on.
                client.callProcedure("@UpdateApplicationCatalog", params);
                response.setStatus(HttpServletResponse.SC_NO_CONTENT);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.SUCCESS, "User removed"));
                notifyOfCatalogUpdate();
            } catch (Exception ex) {
                logger.error("Failed to update role from API", ex);
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, ex.toString()));
            }
        }

        //Handle GET for users
        public void handleGetUsers(String jsonp, String target,
                           Request baseRequest,
                           HttpServletRequest request,
                           HttpServletResponse response, Client client)
                           throws IOException, ServletException {
            ObjectMapper mapper = new ObjectMapper();
            User user = null;
            String[] splitTarget = target.split("/");
            if (splitTarget.length < 3 || splitTarget[2].isEmpty()) {
                if (jsonp != null) {
                    response.getWriter().write(jsonp + "(");
                }
                if (getDeployment().getUsers() != null) {
                    List<IdUser> id = new ArrayList<IdUser>();
                    for(UsersType.User u : getDeployment().getUsers().getUser()) {
                        id.add(new IdUser(u, getHostHeader()));
                    }
                    mapper.writeValue(response.getWriter(), id);
                } else {
                    response.getWriter().write("");
                }
                if (jsonp != null) {
                    response.getWriter().write(")");
                }
                return;
            }
            user = findUser(splitTarget[2], getDeployment());
            if (user == null) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "User not found"));
                return;
            } else {
                if (jsonp != null) {
                    response.getWriter().write(jsonp + "(");
                }
                mapper.writeValue(response.getWriter(), new IdUser(user, getHostHeader()));
                if (jsonp != null) {
                    response.getWriter().write(")");
                }
            }
        }

        //Handle GET for export types
        public void handleGetExportTypes(String jsonp, HttpServletResponse response)
                           throws IOException, ServletException {
            if (jsonp != null) {
                response.getWriter().write(jsonp + "(");
            }
            JSONObject exportTypes = new JSONObject();
            HashSet<String> exportList = new HashSet<String>();
            for (ServerExportEnum type : ServerExportEnum.values()) {
                exportList.add(type.value().toUpperCase());
            }
            try {
                exportTypes.put("types", exportList);
            } catch (JSONException e) {
                m_log.error("Failed to generate exportTypes JSON: ", e);
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Type list failed to build"));
                return;
            }
            response.getWriter().write(exportTypes.toString());
            if (jsonp != null) {
                response.getWriter().write(")");
            }
        }
    }

    class APIRequestHandler extends VoltRequestHandler {

        @Override
        public void handle(String target, Request baseRequest,
                           HttpServletRequest request, HttpServletResponse response)
                            throws IOException, ServletException {
            super.handle(target, baseRequest, request, response);
            try {
                // http://www.ietf.org/rfc/rfc4627.txt dictates this mime type
                response.setContentType("application/json;charset=utf-8");
                if (m_jsonEnabled) {
                    httpClientInterface.process(baseRequest, response);

                    // used for perf testing of the http interface
                    /*String msg = "{\"status\":1,\"appstatus\":-128,\"statusstring\":null,\"appstatusstring\":null,\"exception\":null,\"results\":[{\"status\":-128,\"schema\":[{\"name\":\"SVAL1\",\"type\":9},{\"name\":\"SVAL2\",\"type\":9},{\"name\":\"SVAL3\",\"type\":9}],\"data\":[[\"FOO\",\"BAR\",\"BOO\"]]}]}";
                    response.setStatus(HttpServletResponse.SC_OK);
                    baseRequest.setHandled(true);
                    response.getWriter().print(msg);*/
                } else {
                    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                    baseRequest.setHandled(true);
                    response.getWriter().println("JSON API IS CURRENTLY DISABLED");
                }

            } catch(Exception ex){
                logger.info("Not servicing url: " + baseRequest.getRequestURI() + " Details: "+ ex.getMessage(), ex);
            }
        }
    }

    /**
     * Draw the catalog report page, mostly by pulling it from the JAR.
     */
    void handleReportPage(Request baseRequest, HttpServletResponse response) {
        try {
            String report = ReportMaker.liveReport();

            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);

            response.getWriter().print(report);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
            boolean jsonEnabled, String intf, int port, boolean mustListen) throws Exception {
        int poolsize = Integer.getInteger("HTTP_POOL_SIZE", 50);
        int timeout = Integer.getInteger("HTTP_REQUEST_TIMEOUT_SECONDS", 15);

        /*
         * Don't force us to look at a huge pile of threads
         */
        final QueuedThreadPool qtp = new QueuedThreadPool(poolsize);
        qtp.setIdleTimeout(timeout * 1000);
        qtp.setMinThreads(1);

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
            // The socket channel connector seems to be faster for our use
            //SelectChannelConnector connector = new SelectChannelConnector();
            connector = new ServerConnector(m_server);

            if (intf != null && intf.length() > 0) {
                connector.setHost(intf);
            }
            connector.setPort(port);
            connector.setName("VoltDB-HTTPD");
            //open the connector here so we know if port is available and Init work can retry with next port.
            connector.open();
            m_server.addConnector(connector);

            //"/"
            ContextHandler dbMonitorHandler = new ContextHandler("/");
            dbMonitorHandler.setHandler(new DBMonitorHandler());

            ///api/1.0/
            ContextHandler apiRequestHandler = new ContextHandler("/api/1.0");
            // the default is 200k which well short of out 2M row size limit
            apiRequestHandler.setMaxFormContentSize(HTTPClientInterface.MAX_QUERY_PARAM_SIZE);
            apiRequestHandler.setMaxFormKeys(HTTPClientInterface.MAX_FORM_KEYS);
            apiRequestHandler.setHandler(new APIRequestHandler());

            ///catalog
            ContextHandler catalogRequestHandler = new ContextHandler("/catalog");
            catalogRequestHandler.setHandler(new CatalogRequestHandler());

            ///catalog
            ContextHandler ddlRequestHandler = new ContextHandler("/ddl");
            ddlRequestHandler.setHandler(new DDLRequestHandler());

            ///deployment
            ContextHandler deploymentRequestHandler = new ContextHandler("/deployment");
            m_deploymentHandler = new DeploymentRequestHandler();
            deploymentRequestHandler.setHandler(m_deploymentHandler);

            ///profile
            ContextHandler profileRequestHandler = new ContextHandler("/profile");
            profileRequestHandler.setHandler(new UserProfileHandler());

            ContextHandlerCollection handlers = new ContextHandlerCollection();
            handlers.setHandlers(new Handler[] {
                    apiRequestHandler,
                    catalogRequestHandler,
                    ddlRequestHandler,
                    deploymentRequestHandler,
                    profileRequestHandler,
                    dbMonitorHandler
            });

            m_server.setHandler(handlers);

            httpClientInterface.setTimeout(timeout);
            m_jsonEnabled = jsonEnabled;
        } catch (Exception e) {
            // double try to make sure the port doesn't get eaten
            try { connector.close(); } catch (Exception e2) {}
            try { m_server.destroy(); } catch (Exception e2) {}
            throw new Exception(e);
        }
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
        httpClientInterface.stop();

        try {
            m_server.stop();
            m_server.join();
        }
        catch (Exception e) {}
        try { m_server.destroy(); } catch (Exception e2) {}
        m_server = null;
    }

    public void notifyOfCatalogUpdate()
    {
        //Notify to clean any cached clients so new security can be enforced.
        httpClientInterface.notifyOfCatalogUpdate();
    }
}
