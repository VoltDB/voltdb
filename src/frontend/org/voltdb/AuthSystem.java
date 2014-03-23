/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb;

import static org.voltdb.common.Constants.AUTH_HANDSHAKE;
import static org.voltdb.common.Constants.AUTH_HANDSHAKE_VERSION;
import static org.voltdb.common.Constants.AUTH_SERVICE_NAME;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.AccountExpiredException;
import javax.security.auth.login.CredentialExpiredException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.mindrot.BCrypt;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.security.AuthenticationRequest;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;


/**
 * The AuthSystem parses authentication and permission information from the catalog and uses it to generate a representation
 * of the permissions assigned to users and groups.
 *
 */
public class AuthSystem {

    private static final VoltLogger authLogger = new VoltLogger("AUTH");

    /**
     * JASS Login configuration entry designator
     */
    public static final String VOLTDB_SERVICE_LOGIN_MODULE =
            System.getProperty("VOLTDB_SERVICE_LOGIN_MODULE", "VoltDBService");

    /**
     * Authentication provider enumeration. It serves also as mapping mechanism
     * for providers, which are configured in the deployment file, and the login
     * packet service field.
     */
    public enum AuthProvider {
        HASH("hash","database"),
        KERBEROS("kerberos","kerberos");

        private final static Map<String,AuthProvider> providerMap;
        private final static Map<String,AuthProvider> serviceMap;

        static {
            ImmutableMap.Builder<String, AuthProvider> pbldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, AuthProvider> sbldr = ImmutableMap.builder();
            for (AuthProvider ap: values()) {
                pbldr.put(ap.provider,ap);
                sbldr.put(ap.service,ap);
            }
            providerMap = pbldr.build();
            serviceMap = sbldr.build();
        }

        final String provider;
        final String service;

        AuthProvider(String provider, String service) {
            this.provider = provider;
            this.service = service;
        }

        /**
         * @return its security provider equivalent
         */
        public String provider() {
            return provider;
        }

        /**
         * @return its login packet service equivalent
         */
        public String service() {
            return service;
        }

        public static AuthProvider fromProvider(String provider) {
            AuthProvider ap = providerMap.get(provider);
            if (ap == null) {
                throw new IllegalArgumentException("No provider mapping for " + provider);
            }
            return ap;
        }

        public static AuthProvider fromService(String service) {
            AuthProvider ap = serviceMap.get(service);
            if (ap == null) {
                throw new IllegalArgumentException("No service mapping for " + service);
            }
            return ap;
        }
    }

    /**
     * Representation of a permission group.
     *
     */
    class AuthGroup {
        /**
         * Name of the group
         */
        private final String m_name;

        /**
         * Set of users that are a member of this group
         */
        private Set<AuthUser> m_users = new HashSet<AuthUser>();

        /**
         * Whether membership in this group grants permission to invoke system procedures
         */
        private final boolean m_sysproc;

        /**
         * Whether membership in this group grants permission to invoke default procedures
         */
        private final boolean m_defaultproc;

        /**
         * Whether membership in this group grants permission to invoke adhoc queries
         */
        private final boolean m_adhoc;

        /**
         *
         * @param name Name of the group
         * @param sysproc Whether membership in this group grants permission to invoke system procedures
         * @param defaultproc Whether membership in this group grants permission to invoke default procedures
         * @param adhoc Whether membership in this group grants permission to invoke adhoc queries
         */
        private AuthGroup(String name, boolean sysproc, boolean defaultproc, boolean adhoc) {
            m_name = name.intern();
            m_sysproc = sysproc;
            m_defaultproc = defaultproc;
            m_adhoc = adhoc;
        }

        private void finish() {
            m_users = ImmutableSet.copyOf(m_users);
        }
    }

    /**
     * Representation of the permissions associated with a specific user along with a SHA-1 double hashed copy of the users
     * clear text password.
     *
     */
    class AuthUser {
        /**
         * SHA-1 double hashed copy of the users clear text password
         */
        private final byte[] m_sha1ShadowPassword;

        /**
         * SHA-1 hashed and then bcrypted copy of the users clear text password
         */
        private final String m_bcryptShadowPassword;

        /**
         * Name of the user
         */
        public final String m_name;

        /**
         * Whether this user is granted permission to invoke system procedures (can also be granted by group membership)
         */
        private final boolean m_sysproc;

        /**
         * Whether this user is granted permission to invoke default procedures (can also be granted by group membership)
         */
        private final boolean m_defaultproc;

        /**
         * Whether this user is granted permission to invoke adhoc queries (can also be granted by group membership)
         */
        private final boolean m_adhoc;

        /**
         * Fast iterable list of groups this user is a member of.
         */
        private List<AuthGroup> m_groups = new ArrayList<AuthGroup>();

        /**
         * Fast membership check set of stored procedures this user has permission to invoke.
         * This is generated when the catalog is parsed and it includes procedures the user has permission
         * to invoke by virtue of group membership. The catalog entry for the stored procedure is used here.
         */
        private Set<Procedure> m_authorizedProcedures = new HashSet<Procedure>();

        /**
         * Set of export connectors this user is authorized to access.
         */
        private Set<Connector> m_authorizedConnectors = new HashSet<Connector>();

        /**
         * The constructor accepts the password as either sha1 or bcrypt. In practice
         * there will be only one passed in depending on the format of the password in the catalog.
         * The other will be null and that is used to determine how to hash the supplied password
         * for auth
         * @param shadowPassword SHA-1 double hashed copy of the users clear text password
         * @param name Name of the user
         * @param sysproc Whether this user is granted permission to invoke system procedures (can also be granted by group membership)
         * @param defaultproc Whether this user is granted permission to invoke default procedures (can also be granted by group membership)
         * @param adhoc Whether this user is granted permission to invoke adhoc queries (can also be granted by group membership)
         */
        private AuthUser(byte[] sha1ShadowPassword, String bcryptShadowPassword, String name,
                         boolean sysproc, boolean defaultproc, boolean adhoc) {
            m_sha1ShadowPassword = sha1ShadowPassword;
            m_bcryptShadowPassword = bcryptShadowPassword;
            if (name != null) {
                m_name = name.intern();
            } else {
                m_name = null;
            }
            m_sysproc = sysproc;
            m_defaultproc = defaultproc;
            m_adhoc = adhoc;
        }

        /**
         * Check if a user has permission to invoke the specified stored procedure
         * Handle both user-written procedures and default auto-generated ones.
         * @param proc Catalog entry for the stored procedure to check
         * @return true if the user has permission and false otherwise
         */
        public boolean hasPermission(Procedure proc) {
            if (proc == null) {
                return false;
            }
            if (proc.getDefaultproc()) {
                return hasDefaultProcPermission();
            }
            return m_authorizedProcedures.contains(proc);
        }

        /**
         * Check if a user has permission to invoke adhoc queries by virtue of a direct grant, group membership,
         * or having permission to invoke system procedures of which adhoc is one.
         * @return true if the user has permission and false otherwise
         */
        public boolean hasAdhocPermission() {
            return m_adhoc || hasGroupWithAdhocPermission() || hasSystemProcPermission();
        }

        /**
         * Utility function to iterate through groups and check if any group the user is a member of
         * grants adhoc permission
         * @return true if the user has permission and false otherwise
         */
        private boolean hasGroupWithAdhocPermission() {
            for (AuthGroup group : m_groups) {
                if (group.m_adhoc) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Check if a user has permission to invoke system procedures by virtue of a direct grant, or group membership,
         * @return true if the user has permission and false otherwise
         */
        public boolean hasSystemProcPermission() {
            return m_sysproc || hasGroupWithSysProcPermission();
        }

        /**
         * Check if a user has permission to invoke default procedures by virtue of a direct grant, or group membership,
         * @return true if the user has permission and false otherwise
         */
        public boolean hasDefaultProcPermission() {
            return m_defaultproc || hasGroupWithDefaultProcPermission();
        }

        /**
         * Utility function to iterate through groups and check if any group the user is a member of
         * grants sysproc permission
         * @return true if the user has permission and false otherwise
         */
        private boolean hasGroupWithSysProcPermission() {
            for (AuthGroup group : m_groups) {
                if (group.m_sysproc) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Utility function to iterate through groups and check if any group the user is a member of
         * grants defaultproc permission
         * @return true if the user has permission and false otherwise
         */
        private boolean hasGroupWithDefaultProcPermission() {
            for (AuthGroup group : m_groups) {
                if (group.m_defaultproc) {
                    return true;
                }
            }
            return false;
        }

        public boolean authorizeConnector(String connectorClass) {
            if (connectorClass == null) {
                return false;
            }
            for (Connector c : m_authorizedConnectors) {
                if (c.getLoaderclass().equals(connectorClass)) {
                    return true;
                }
            }
            return false;
        }

        private void finish() {
            m_groups = ImmutableList.copyOf(m_groups);
            m_authorizedProcedures = ImmutableSet.copyOf(m_authorizedProcedures);
            m_authorizedConnectors = ImmutableSet.copyOf(m_authorizedConnectors);
        }
    }

    /**
     * Storage for user permissions keyed on the username
     */
    private Map<String, AuthUser> m_users = new HashMap<String, AuthUser>();

    /**
     * Storage for group permissions keyed on group name.
     */
    private Map<String, AuthGroup> m_groups = new HashMap<String, AuthGroup>();

    /**
     * Indicates whether security is enabled. If security is disabled all authentications will succede and all returned
     * AuthUsers will allow everything.
     */
    private final boolean m_enabled;

    /**
     * The configured authentication provider
     */
    private final AuthProvider m_authProvider;

    /**
     * VoltDB Kerberos service login context
     */
    private final LoginContext m_loginCtx;

    /**
     * VoltDB service principal name
     */
    private final byte [] m_principalName;

    private final GSSManager m_gssManager;

    AuthSystem(final Database db, boolean enabled) {
        AuthProvider ap = null;
        LoginContext loginContext = null;
        GSSManager gssManager = null;
        byte [] principal = null;

        m_enabled = enabled;
        if (!m_enabled) {
            m_authProvider = ap;
            m_loginCtx = loginContext;
            m_principalName = principal;
            m_gssManager = null;
            return;
        }
        m_authProvider = AuthProvider.fromProvider(db.getSecurityprovider());
        if (m_authProvider == AuthProvider.KERBEROS) {
            try {
                loginContext = new LoginContext(VOLTDB_SERVICE_LOGIN_MODULE);
            } catch (LoginException|SecurityException ex) {
                VoltDB.crashGlobalVoltDB(
                        "Cannot initialize JAAS LoginContext", true, ex);
            }
            try {
                loginContext.login();
                principal = loginContext
                        .getSubject()
                        .getPrincipals()
                        .iterator().next()
                        .getName()
                        .getBytes(Charsets.UTF_8)
                        ;
                gssManager = GSSManager.getInstance();
            } catch (AccountExpiredException ex) {
                VoltDB.crashGlobalVoltDB(
                        "VoltDB assigned service principal has expired", true, ex);
            } catch(CredentialExpiredException ex) {
                VoltDB.crashGlobalVoltDB(
                        "VoltDB assigned service principal credentials have expired", true, ex);
            } catch(FailedLoginException ex) {
                VoltDB.crashGlobalVoltDB(
                        "VoltDB failed to authenticate against kerberos", true, ex);
            }
            catch (LoginException ex) {
                VoltDB.crashGlobalVoltDB(
                        "VoltDB service principal failed to login", true, ex);
            }
            catch (Exception ex) {
                VoltDB.crashGlobalVoltDB(
                        "Unexpected exception occured during service authentication", true, ex);
            }
        }
        m_loginCtx = loginContext;
        m_principalName = principal;
        m_gssManager = gssManager;
        /*
         * First associate all users with groups and vice versa
         */
        for (org.voltdb.catalog.User catalogUser : db.getUsers()) {
            String shadowPassword = catalogUser.getShadowpassword();
            byte sha1ShadowPassword[] = null;
            if (shadowPassword.length() == 40) {
                /*
                 * This is an old catalog with a SHA-1 password
                 * Need to hex decode it
                 */
                sha1ShadowPassword = Encoder.hexDecode(shadowPassword);
            } else if (shadowPassword.length() != 60) {
                /*
                 * If not 40 should be 60 since it is bcrypt
                 */
                VoltDB.crashGlobalVoltDB(
                        "Found a shadowPassword in the catalog that was in an unrecogized format", true, null);
            }

            final AuthUser user = new AuthUser( sha1ShadowPassword, shadowPassword,
                    catalogUser.getTypeName(), catalogUser.getSysproc(),
                    catalogUser.getDefaultproc(), catalogUser.getAdhoc());
            m_users.put(user.m_name, user);
            for (org.voltdb.catalog.GroupRef catalogGroupRef : catalogUser.getGroups()) {
                final org.voltdb.catalog.Group  catalogGroup = catalogGroupRef.getGroup();
                AuthGroup group = null;
                if (!m_groups.containsKey(catalogGroup.getTypeName())) {
                    group = new AuthGroup(catalogGroup.getTypeName(), catalogGroup.getSysproc(),
                                          catalogGroup.getDefaultproc(), catalogGroup.getAdhoc());
                    m_groups.put(group.m_name, group);
                } else {
                    group = m_groups.get(catalogGroup.getTypeName());
                }
                group.m_users.add(user);
                user.m_groups.add(group);
            }
        }

        for (org.voltdb.catalog.Group catalogGroup : db.getGroups()) {
            AuthGroup group = null;
            if (!m_groups.containsKey(catalogGroup.getTypeName())) {
                group = new AuthGroup(catalogGroup.getTypeName(), catalogGroup.getSysproc(),
                                      catalogGroup. getDefaultproc(), catalogGroup.getAdhoc());
                m_groups.put(group.m_name, group);
                //A group not associated with any users? Weird stuff.
            } else {
                group = m_groups.get(catalogGroup.getTypeName());
            }
        }

        /*
         * Then iterate through each procedure and and add it
         * to the set of procedures for each specified user and for the members of
         * each specified group.
         */
        for (org.voltdb.catalog.Procedure catalogProcedure : db.getProcedures()) {
            for (org.voltdb.catalog.UserRef catalogUserRef : catalogProcedure.getAuthusers()) {
                final org.voltdb.catalog.User catalogUser = catalogUserRef.getUser();
                final AuthUser user = m_users.get(catalogUser.getTypeName());
                if (user == null) {
                    //Error case. Procedure has a user listed as authorized but no such user exists
                } else {
                    user.m_authorizedProcedures.add(catalogProcedure);
                }
            }

            for (org.voltdb.catalog.GroupRef catalogGroupRef : catalogProcedure.getAuthgroups()) {
                final org.voltdb.catalog.Group catalogGroup = catalogGroupRef.getGroup();
                final AuthGroup group = m_groups.get(catalogGroup.getTypeName());
                if (group == null) {
                    //Error case. Procedure has a group listed as authorized but no such user exists
                } else {
                    for (AuthUser user : group.m_users) {
                        user.m_authorizedProcedures.add(catalogProcedure);
                    }
                }
            }
        }

        m_users = ImmutableMap.copyOf(m_users);
        m_groups = ImmutableMap.copyOf(m_groups);
        for (AuthUser user : m_users.values()) {
            user.finish();
        }
        for (AuthGroup group : m_groups.values()) {
            group.finish();
        }
    }

    /**
     * Check the username and password against the catalog. Return the appropriate permission
     * set for that user if the information is correct and return null otherwise. If security is disabled
     * an AuthUser object that grants all permissions is returned.
     * @param username Name of the user to authenticate
     * @param password SHA-1 single hashed version of the users clear text password
     * @return The permission set for the user if authentication succeeds or null if authentication fails.
     */
    boolean authenticate(String username, byte[] password) {
        if (!m_enabled) {
            return true;
        }
        final AuthUser user = m_users.get(username);
        if (user == null) {
            authLogger.l7dlog(Level.INFO, LogKeys.auth_AuthSystem_NoSuchUser.name(), new String[] {username}, null);
            return false;
        }

        boolean matched = true;
        if (user.m_sha1ShadowPassword != null) {
            MessageDigest md = null;
            try {
                md = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }
            byte passwordHash[] = md.digest(password);

            /*
             * A n00bs attempt at constant time comparison
             */
            for (int ii = 0; ii < passwordHash.length; ii++) {
                if (passwordHash[ii] != user.m_sha1ShadowPassword[ii]){
                    matched = false;
                }
            }
        } else {
            matched = BCrypt.checkpw(Encoder.hexEncode(password), user.m_bcryptShadowPassword);
        }

        if (matched) {
            authLogger.l7dlog(Level.INFO, LogKeys.auth_AuthSystem_AuthenticatedUser.name(), new Object[] {username}, null);
            return true;
        } else {
            authLogger.l7dlog(Level.INFO, LogKeys.auth_AuthSystem_AuthFailedPasswordMistmatch.name(), new String[] {username}, null);
            return false;
        }
    }

    private final AuthUser m_authDisabledUser = new AuthUser(null, null, null, false, false, false) {
        @Override
        public boolean hasPermission(Procedure proc) {
            return true;
        }

        @Override
        public boolean hasAdhocPermission() {
            return true;
        }

        @Override
        public boolean hasSystemProcPermission() {
            return true;
        }

        @Override
        public boolean hasDefaultProcPermission() {
            return true;
        }

        @Override
        public boolean authorizeConnector(String connectorName) {
            return true;
        }
    };

    AuthUser getUser(String name) {
        if (!m_enabled) {
            return m_authDisabledUser;
        }
        return m_users.get(name);
    }

    public class HashAuthenticationRequest extends AuthenticationRequest {

        private final String m_user;
        private final byte [] m_password;

        public HashAuthenticationRequest(final String user, final byte [] hash) {
            m_user = user;
            m_password = hash;
        }

        @Override
        protected boolean authenticateImpl() throws Exception {
            if (!m_enabled) {
                m_authenticatedUser = m_user;
                return true;
            }
            else if (m_authProvider != AuthProvider.HASH) {
                return false;
            }
            final AuthUser user = m_users.get(m_user);
            if (user == null) {
                authLogger.l7dlog(Level.INFO, LogKeys.auth_AuthSystem_NoSuchUser.name(), new String[] {m_user}, null);
                return false;
            }

            boolean matched = true;
            if (user.m_sha1ShadowPassword != null) {
                MessageDigest md = null;
                try {
                    md = MessageDigest.getInstance("SHA-1");
                } catch (NoSuchAlgorithmException e) {
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }
                byte passwordHash[] = md.digest(m_password);

                /*
                 * A n00bs attempt at constant time comparison
                 */
                for (int ii = 0; ii < passwordHash.length; ii++) {
                    if (passwordHash[ii] != user.m_sha1ShadowPassword[ii]){
                        matched = false;
                    }
                }
            } else {
                matched = BCrypt.checkpw(Encoder.hexEncode(m_password), user.m_bcryptShadowPassword);
            }

            if (matched) {
                authLogger.l7dlog(Level.INFO, LogKeys.auth_AuthSystem_AuthenticatedUser.name(), new Object[] {m_user}, null);
                m_authenticatedUser = m_user;
                return true;
            } else {
                authLogger.l7dlog(Level.INFO, LogKeys.auth_AuthSystem_AuthFailedPasswordMistmatch.name(), new String[] {m_user}, null);
                return false;
            }
        }
    }

    public class KerberosAuthenticationRequest extends AuthenticationRequest {
        private SocketChannel m_socket;
        public KerberosAuthenticationRequest(final SocketChannel socket) {
            m_socket = socket;
        }
        @Override
        protected boolean authenticateImpl() throws Exception {
            if (!m_enabled) {
                m_authenticatedUser = "_^_pinco_pallo_^_";
                return true;
            }
            else if (m_authProvider != AuthProvider.KERBEROS) {
                return false;
            }
            int msgSize =
                      4 // message size header
                    + 1 // protocol version
                    + 1 // result code
                    + 4 // service name length
                    + m_principalName.length;

            final ByteBuffer bb = ByteBuffer.allocate(4096);

            /*
             * write the service principal response. This gives the connecting client
             * the service principal name form which it constructs the GSS context
             * used in the client/service authentication handshake
             */
            bb.putInt(msgSize-4).put(AUTH_HANDSHAKE_VERSION).put(AUTH_SERVICE_NAME);
            bb.putInt(m_principalName.length);
            bb.put(m_principalName);
            bb.flip();

            while (bb.hasRemaining()) {
                m_socket.write(bb);
            }

            String authenticatedUser = Subject.doAs(m_loginCtx.getSubject(), new PrivilegedAction<String>() {
                /**
                 * Establish an authenticated GSS security context
                 * For further information on GSS please refer to
                 * <a href="http://en.wikipedia.org/wiki/Generic_Security_Services_Application_Program_Interface">this</a>
                 * article on Generic Security Services Application Program Interface
                 */
                @Override
                public String run() {
                    GSSContext context = null;
                    try {
                        // derive the credentials from the authenticated service subject
                        context = m_gssManager.createContext((GSSCredential)null);
                        byte [] token;

                        while (!context.isEstablished()) {
                            // read in the next packet size
                            bb.clear().limit(4);
                            while (bb.hasRemaining()) {
                                if (m_socket.read(bb) == -1) throw new EOFException();
                            }
                            bb.flip();

                            int msgSize = bb.getInt();
                            if (msgSize > bb.capacity()) {
                                authLogger.warn("Authentication packet exceeded alloted size");
                                return null;
                            }
                            // read the initiator (client) context token
                            bb.clear().limit(msgSize);
                            while (bb.hasRemaining()) {
                                if (m_socket.read(bb) == -1) throw new EOFException();
                            }
                            bb.flip();

                            byte version = bb.get();
                            if (version != AUTH_HANDSHAKE_VERSION) {
                                authLogger.warn("Encountered unexpected authentication protocol version " + version);
                                return null;
                            }

                            byte tag = bb.get();
                            if (tag != AUTH_HANDSHAKE) {
                                authLogger.warn("Encountered unexpected authentication protocol tag " + tag);
                                return null;
                            }

                            // process the initiator (client) context token. If it returns a non empty token
                            // transmit it to the initiator
                            token = context.acceptSecContext(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
                            if (token != null) {
                                msgSize = 4 + 1 + 1 + token.length;
                                bb.clear().limit(msgSize);
                                bb.putInt(msgSize-4).put(AUTH_HANDSHAKE_VERSION).put(AUTH_HANDSHAKE);
                                bb.put(token);
                                bb.flip();

                                while (bb.hasRemaining()) {
                                    m_socket.write(bb);
                                }
                            }
                        }
                        // at this juncture we an established security context between
                        // the client and this service
                        String authenticateUserName = context.getSrcName().toString();

                        // check if both ends are authenticated
                        if (!context.getMutualAuthState()) {
                            return null;
                        }
                        context.dispose();
                        context = null;

                        return authenticateUserName;

                    } catch (IOException|GSSException ex) {
                        Throwables.propagate(ex);
                    } finally {
                        if (context != null) try { context.dispose(); } catch (Exception ignoreIt) {}
                    }
                    return null;
                }
            });

            if (authenticatedUser != null) {
                final AuthUser user = m_users.get(authenticatedUser);
                if (user == null) {
                    authLogger.l7dlog(Level.INFO, LogKeys.auth_AuthSystem_NoSuchUser.name(), new String[] {authenticatedUser}, null);
                    return false;
                }
                m_authenticatedUser = authenticatedUser;
                authLogger.l7dlog(Level.INFO, LogKeys.auth_AuthSystem_AuthenticatedUser.name(), new Object[] {authenticatedUser}, null);
            }

            return true;
        }
    }
}
