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

import com.google_voltpatches.common.base.Throwables;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.annotation.XmlAttribute;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsonschema.JsonSchema;
import org.eclipse.jetty.server.Request;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.AuthenticationResult;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.HTTPClientInterface;
import org.voltdb.VoltDB;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.SyncCallback;
import org.voltdb.common.Permission;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.ExportType;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.compiler.deploymentfile.UsersType.User;
import static org.voltdb.utils.HTTPAdminListener.JSON_CONTENT_TYPE;

/**
 *
 * Servers /deployment and sub resources also supports updating deployment via REST.
 */
public class DeploymentRequestServlet extends VoltBaseServlet {

    private static final long serialVersionUID = -1160628893809713993L;

    String m_schema = "";

    final ObjectMapper m_mapper = MapperHolder.mapper;
    final ObjectMapper m_updatemapper = MapperHolder.m_updatemapper;
    // ObjectMapper is thread safe, and uses a lot of memory to cache
    // class specific serializers and deserializers. Use JSR-133
    // initialization on demand holder to hold a sole instance
    public final static class MapperHolder {
        final static public ObjectMapper mapper;
        final static public ObjectMapper m_updatemapper;
        final static public JsonFactory factory = new JsonFactory();
        static {
            ObjectMapper configurable = new ObjectMapper();
            // configurable.setSerializationInclusion(Inclusion.NON_NULL);
            configurable.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            configurable.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
            configurable.addMixIn(UsersType.User.class, IgnorePasswordMixIn.class);
            configurable.addMixIn(ExportType.class, IgnoreLegacyExportAttributesMixIn.class);
            //These mixins are to ignore the "key" and redirect "path" to getNodePath()
            configurable.addMixIn(PathsType.Commandlog.class,
                    IgnoreNodePathKeyMixIn.class);
            configurable.addMixIn(PathsType.Commandlogsnapshot.class,
                    IgnoreNodePathKeyMixIn.class);
            configurable.addMixIn(PathsType.Droverflow.class,
                    IgnoreNodePathKeyMixIn.class);
            configurable.addMixIn(PathsType.Exportoverflow.class,
                    IgnoreNodePathKeyMixIn.class);
            configurable.addMixIn(PathsType.Snapshots.class,
                    IgnoreNodePathKeyMixIn.class);
            configurable.addMixIn(PathsType.Voltdbroot.class, IgnoreNodePathKeyMixIn.class);

            mapper = configurable;

            ObjectMapper uconfigurable = new ObjectMapper();
            // configurable.setSerializationInclusion(Inclusion.NON_NULL);
            uconfigurable.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            uconfigurable.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
            uconfigurable.addMixIn(ExportType.class, IgnoreLegacyExportAttributesMixIn.class);
            //These mixins are to ignore the "key" and redirect "path" to getNodePath()
            uconfigurable.addMixIn(PathsType.Commandlog.class,
                    IgnoreNodePathKeyMixIn.class);
            uconfigurable.addMixIn(PathsType.Commandlogsnapshot.class,
                    IgnoreNodePathKeyMixIn.class);
            uconfigurable.addMixIn(PathsType.Droverflow.class,
                    IgnoreNodePathKeyMixIn.class);
            uconfigurable.addMixIn(PathsType.Exportoverflow.class,
                    IgnoreNodePathKeyMixIn.class);
            uconfigurable.addMixIn(PathsType.Snapshots.class,
                    IgnoreNodePathKeyMixIn.class);
            uconfigurable.addMixIn(PathsType.Voltdbroot.class, IgnoreNodePathKeyMixIn.class);
            m_updatemapper = uconfigurable;
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
    abstract class IgnoreNodePathKeyMixIn {
        @JsonProperty("path") abstract String getNodePath();
        @JsonIgnore abstract String getKey();
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

    @Override
    public void init() {

    }

    public DeploymentRequestServlet() {
        try {
            JsonSchema schema = m_mapper.generateJsonSchema(DeploymentType.class);
            m_schema = schema.toString();
        } catch (JsonMappingException ex) {
            m_log.warn("Failed to generate JSON schema: ", ex);
        }
    }

    private CatalogContext getCatalogContext() {
        return VoltDB.instance().getCatalogContext();
    }

    //Get deployment from catalog context
    private DeploymentType getDeployment() {
        //If running with new verbs add runtime paths.
        DeploymentType dt = CatalogUtil.updateRuntimeDeploymentPaths(getCatalogContext().getDeployment());
        return dt;
    }

    //Get deployment bytes from catalog context
    private byte[] getDeploymentBytes() {
        return VoltDB.instance().getCatalogContext().getDeploymentBytes();
    }

    private UsersType.User findUser(String user, DeploymentType dep) {
        if (dep.getUsers() != null) {
            for (User u : dep.getUsers().getUser()) {
                if (user.equalsIgnoreCase(u.getName())) {
                    return u;
                }
            }
        }
        return null;
    }

    @Override
    public void doDelete(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        doGet(request, response);
    }

    @Override
    public void doPut(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        doGet(request, response);
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
    public void doGet(HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        super.doGet(request, response);

        //jsonp is specified when response is expected to go to javascript function.
        String jsonp = request.getParameter(HTTPClientInterface.JSONP);
        AuthenticationResult authResult = null;
        String target = request.getPathInfo();
        try {
            response.setContentType(JSON_CONTENT_TYPE);
            if (!HTTPClientInterface.validateJSONP(jsonp, (Request )request, response)) {
                return;
            }
            response.setStatus(HttpServletResponse.SC_OK);

            //Requests require authentication.
            authResult = authenticate(request);
            if (!authResult.isAuthenticated()) {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, authResult.m_message));
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                return;
            }
            //Authenticated but has no permissions.
            if (!authResult.m_authUser.hasPermission(Permission.ADMIN)) {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Permission denied"));
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                return;
            }

            if (target != null && !target.endsWith("/")) { // the URI may or may not end with /
                target += "/";
            }
            if (target == null) {
                target = "/"; //Default.
            }
            //Authenticated and has ADMIN permission
            if (target.equals("/download/")) {
                //Deployment xml is text/xml
                response.setContentType("text/xml;charset=utf-8");
                DeploymentType dt = CatalogUtil.shallowClusterAndPathsClone(this.getDeployment());
                // reflect the actual number of cluster members
                dt.getCluster().setHostcount(getCatalogContext().getClusterSettings().hostcount());

                response.getWriter().write(CatalogUtil.getDeployment(dt, true));
            } else if (target.startsWith("/users/")) { // username may be passed in after the / (not as a param)
                if (request.getMethod().equalsIgnoreCase("POST")) {
                    handleUpdateUser(jsonp, target, request, response, authResult);
                } else if (request.getMethod().equalsIgnoreCase("PUT")) {
                    handleCreateUser(jsonp, target, request, response, authResult);
                } else if (request.getMethod().equalsIgnoreCase("DELETE")) {
                    handleRemoveUser(jsonp, target, request, response, authResult);
                } else {
                    handleGetUsers(jsonp, target, request, response);
                }
            } else if (target.equals("/export/types/")) {
                handleGetExportTypes(jsonp, response);
            } else if (target.equals("/")) { // just deployment
                if (request.getMethod().equalsIgnoreCase("POST")) {
                    handleUpdateDeployment(jsonp, request, response, authResult);
                } else {
                    //non POST
                    response.setCharacterEncoding("UTF-8");
                    if (jsonp != null) {
                        response.getWriter().write(jsonp + "(");
                    }
                    DeploymentType dt = getDeployment();
                    // reflect the actual number of cluster members
                    dt.getCluster().setHostcount(getCatalogContext().getClusterSettings().hostcount());

                    m_mapper.writeValue(response.getWriter(), dt);
                    if (jsonp != null) {
                        response.getWriter().write(")");
                    }
                }
            } else {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Resource not found"));
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        } catch (Exception ex) {
            m_log.info("Not servicing url: " + target + " Details: " + ex.getMessage(), ex);
        }
    }

    //Update the deployment
    public void handleUpdateDeployment(String jsonp,
            HttpServletRequest request,
            HttpServletResponse response, AuthenticationResult ar)
            throws IOException, ServletException {
        String deployment = request.getParameter("deployment");
        if (deployment == null || deployment.length() == 0) {
            response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to get deployment information."));
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
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
            // reset the host count so that it wont fail the deployment checks
            newDeployment.getCluster().setHostcount(currentDeployment.getCluster().getHostcount());

            String dep = CatalogUtil.getDeployment(newDeployment);
            if (dep == null || dep.trim().length() <= 0) {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to build deployment information."));
                return;
            }
            Object[] params = new Object[]{null, dep};
            SyncCallback cb = new SyncCallback();
            httpClientInterface.callProcedure(request.getRemoteHost(), ar, BatchTimeoutOverrideType.NO_TIMEOUT, cb, "@UpdateApplicationCatalog", params);
            cb.waitForResponse();
            ClientResponseImpl r = ClientResponseImpl.class.cast(cb.getResponse());
            if (r.getStatus() == ClientResponse.SUCCESS) {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.SUCCESS, "Deployment Updated."));
            } else {
                response.getWriter().print(HTTPClientInterface.asJsonp(jsonp, r.toJSONString()));
            }
        } catch (JsonParseException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Unparsable JSON"));
        } catch (Exception ex) {
            m_log.error("Failed to update deployment from API", ex);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, Throwables.getStackTraceAsString(ex)));
        }
    }

    //Handle POST for users
    public void handleUpdateUser(String jsonp, String target,
            HttpServletRequest request,
            HttpServletResponse response, AuthenticationResult ar)
            throws IOException, ServletException {
        String update = request.getParameter("user");
        if (update == null || update.trim().length() == 0) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to get user information."));
            return;
        }
        try {
            User newUser = m_updatemapper.readValue(update, User.class);
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
            Object[] params = new Object[]{null, dep};
            //Call sync as nothing else can happen when this is going on.
            SyncCallback cb = new SyncCallback();
            httpClientInterface.callProcedure(request.getRemoteHost(), ar, BatchTimeoutOverrideType.NO_TIMEOUT, cb, "@UpdateApplicationCatalog", params);
            cb.waitForResponse();
            ClientResponseImpl r = ClientResponseImpl.class.cast(cb.getResponse());
            if (r.getStatus() == ClientResponse.SUCCESS) {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.SUCCESS, "User Updated."));
            } else {
                response.getWriter().print(HTTPClientInterface.asJsonp(jsonp, r.toJSONString()));
            }

        } catch (Exception ex) {
            m_log.error("Failed to update user from API", ex);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, Throwables.getStackTraceAsString(ex)));
        }
    }

    //Handle PUT for users
    public void handleCreateUser(String jsonp, String target,
            HttpServletRequest request,
            HttpServletResponse response, AuthenticationResult ar)
            throws IOException, ServletException {
        String update = request.getParameter("user");
        if (update == null || update.trim().length() == 0) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, "Failed to get user information."));
            return;
        }
        try {
            User newUser = m_updatemapper.readValue(update, User.class);
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
            Object[] params = new Object[]{null, dep};
            //Call sync as nothing else can happen when this is going on.
            SyncCallback cb = new SyncCallback();
            httpClientInterface.callProcedure(request.getRemoteHost(), ar, BatchTimeoutOverrideType.NO_TIMEOUT, cb, "@UpdateApplicationCatalog", params);
            cb.waitForResponse();
            ClientResponseImpl r = ClientResponseImpl.class.cast(cb.getResponse());
            if (r.getStatus() == ClientResponse.SUCCESS) {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.SUCCESS, returnString));
            } else {
                response.getWriter().print(HTTPClientInterface.asJsonp(jsonp, r.toJSONString()));
            }
        } catch (Exception ex) {
            m_log.error("Failed to create user from API", ex);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, Throwables.getStackTraceAsString(ex)));
        }
    }

    //Handle DELETE for users
    public void handleRemoveUser(String jsonp, String target,
            HttpServletRequest request,
            HttpServletResponse response, AuthenticationResult ar)
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
            Object[] params = new Object[]{null, dep};
            //Call sync as nothing else can happen when this is going on.
            SyncCallback cb = new SyncCallback();
            httpClientInterface.callProcedure(request.getRemoteHost(), ar, BatchTimeoutOverrideType.NO_TIMEOUT, cb, "@UpdateApplicationCatalog", params);
            cb.waitForResponse();
            ClientResponseImpl r = ClientResponseImpl.class.cast(cb.getResponse());
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            if (r.getStatus() == ClientResponse.SUCCESS) {
                response.getWriter().print(buildClientResponse(jsonp, ClientResponse.SUCCESS, "User Removed."));
            } else {
                response.getWriter().print(HTTPClientInterface.asJsonp(jsonp, r.toJSONString()));
            }
        } catch (Exception ex) {
            m_log.error("Failed to update role from API", ex);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().print(buildClientResponse(jsonp, ClientResponse.UNEXPECTED_FAILURE, Throwables.getStackTraceAsString(ex)));
        }
    }

    //Handle GET for users
    public void handleGetUsers(String jsonp, String target,
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {
        ObjectMapper mapper = new ObjectMapper();
        User user = null;
        String[] splitTarget = target.split("/");
        if (splitTarget.length < 3 || splitTarget[2].isEmpty()) {
            if (jsonp != null) {
                response.getWriter().write(jsonp + "(");
            }
            if (getDeployment().getUsers() != null) {
                List<IdUser> id = new ArrayList<>();
                for (UsersType.User u : getDeployment().getUsers().getUser()) {
                    id.add(new IdUser(u, getHostHeader()));
                }
                mapper.writeValue(response.getWriter(), id);
            } else {
                response.getWriter().write("[]");
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
        HashSet<String> exportList = new HashSet<>();
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
