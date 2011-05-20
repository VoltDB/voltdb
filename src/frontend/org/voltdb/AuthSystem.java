/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.voltdb.catalog.Connector;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;


/**
 * The AuthSystem parses authentication and permission information from the catalog and uses it to generate a representation
 * of the permissions assigned to users and groups.
 *
 */
public class AuthSystem {

    private static final VoltLogger authLogger = new VoltLogger("AUTH");

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
        private final HashSet<AuthUser> m_users = new HashSet<AuthUser>();

        /**
         * Whether membership in this group grants permission to invoke system procedures
         */
        private final boolean m_sysproc;

        /**
         * Whether membership in this group grants permission to invoke adhoc queries
         */
        private final boolean m_adhoc;

        /**
         *
         * @param name Name of the group
         * @param sysproc Whether membership in this group grants permission to invoke system procedures
         * @param adhoc Whether membership in this group grants permission to invoke adhoc queries
         */
        private AuthGroup(String name, boolean sysproc, boolean adhoc) {
            m_name = name;
            m_sysproc = sysproc;
            m_adhoc = adhoc;
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
        private final byte[] m_shadowPassword;

        /**
         * Name of the user
         */
        public final String m_name;

        /**
         * Whether this user is granted permission to invoke system procedures (can also be granted by group membership)
         */
        private final boolean m_sysproc;

        /**
         * Whether this user is granted permission to invoke adhoc queries (can also be granted by group membership)
         */
        private final boolean m_adhoc;

        /**
         * Fast iterable list of groups this user is a member of.
         */
        private final ArrayList<AuthGroup> m_groups = new ArrayList<AuthGroup>();

        /**
         * Fast membership check set of stored procedures this user has permission to invoke.
         * This is generated when the catalog is parsed and it includes procedures the user has permission
         * to invoke by virtue of group membership. The catalog entry for the stored procedure is used here.
         */
        private final HashSet<Procedure> m_authorizedProcedures = new HashSet<Procedure>();

        /**
         * Set of export connectors this user is authorized to access.
         */
        private final HashSet<Connector> m_authorizedConnectors = new HashSet<Connector>();

        /**
         *
         * @param shadowPassword SHA-1 double hashed copy of the users clear text password
         * @param name Name of the user
         * @param sysproc Whether this user is granted permission to invoke system procedures (can also be granted by group membership)
         * @param adhoc Whether this user is granted permission to invoke adhoc queries (can also be granted by group membership)
         */
        private AuthUser(byte[] shadowPassword, String name, boolean sysproc, boolean adhoc) {
            m_shadowPassword = shadowPassword;
            m_name = name;
            m_sysproc = sysproc;
            m_adhoc = adhoc;
        }

        /**
         * Check if a user has permission to invoke the specified stored procedure
         * @param proc Catalog entry for the stored procedure to check
         * @return true if the user has permission and false otherwise
         */
        public boolean hasPermission(Procedure proc) {
            if (proc == null) {
                return false;
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
         * Check if a user has permission to invoke adhoc queries by virtue of a direct grant, or group membership,
         * @return true if the user has permission and false otherwise
         */
        public boolean hasSystemProcPermission() {
            return m_sysproc || hasGroupWithSysProcPermission();
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
    }

    /**
     * Storage for user permissions keyed on the username
     */
    private final HashMap<String, AuthUser> m_users = new HashMap<String, AuthUser>( 16, (float).5);

    /**
     * Storage for group permissions keyed on group name.
     */
    private final HashMap<String, AuthGroup> m_groups = new HashMap<String, AuthGroup>( 16, (float).5);

    /**
     * Indicates whether security is enabled. If security is disabled all authentications will succede and all returned
     * AuthUsers will allow everything.
     */
    private final boolean m_enabled;

    AuthSystem(final Database db, boolean enabled) {
        m_enabled = enabled;
        if (!m_enabled) {
            return;
        }
        /*
         * First associate all users with groups and vice versa
         */
        for (org.voltdb.catalog.User catalogUser : db.getUsers()) {
            final AuthUser user = new AuthUser( Encoder.hexDecode(catalogUser.getShadowpassword()),
                    catalogUser.getTypeName(), catalogUser.getSysproc(), catalogUser.getAdhoc());
            m_users.put(user.m_name.intern(), user);
            for (org.voltdb.catalog.GroupRef catalogGroupRef : catalogUser.getGroups()) {
                final org.voltdb.catalog.Group  catalogGroup = catalogGroupRef.getGroup();
                AuthGroup group = null;
                if (!m_groups.containsKey(catalogGroup.getTypeName())) {
                    group = new AuthGroup(catalogGroup.getTypeName(), catalogGroup.getSysproc(), catalogGroup.getAdhoc());
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
                group = new AuthGroup(catalogGroup.getTypeName(), catalogGroup.getSysproc(), catalogGroup.getAdhoc());
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

        /*
         * Iterate through the export connectors and add each connector to
         * the authorized user objects.
         */
        for (org.voltdb.catalog.Connector connector : db.getConnectors()) {
            for (org.voltdb.catalog.UserRef userRef : connector.getAuthusers()) {
                final org.voltdb.catalog.User catalogUser = userRef.getUser();
                final AuthUser user = m_users.get(catalogUser.getTypeName());
                if (user == null) {
                    //Error case. Procedure has a user listed as authorized but no such user exists
                } else {
                    user.m_authorizedConnectors.add(connector);
                }
            }

            for (org.voltdb.catalog.GroupRef catalogGroupRef : connector.getAuthgroups()) {
                final org.voltdb.catalog.Group catalogGroup = catalogGroupRef.getGroup();
                final AuthGroup group = m_groups.get(catalogGroup.getTypeName());
                if (group == null) {
                    //Error case. Procedure has a group listed as authorized but no such user exists
                } else {
                    for (AuthUser user : group.m_users) {
                        user.m_authorizedConnectors.add(connector);
                    }
                }
            }
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

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            authLogger.l7dlog(Level.FATAL, LogKeys.auth_AuthSystem_NoSuchAlgorithm.name(), e);
            VoltDB.crashVoltDB();
        }
        byte passwordHash[] = md.digest(password);
        if (java.util.Arrays.equals(passwordHash, user.m_shadowPassword)) {
            authLogger.l7dlog(Level.INFO, LogKeys.auth_AuthSystem_AuthenticatedUser.name(), new Object[] {username}, null);
            return true;
        } else {
            authLogger.l7dlog(Level.INFO, LogKeys.auth_AuthSystem_AuthFailedPasswordMistmatch.name(), new String[] {username}, null);
            return false;
        }
    }

    AuthUser getUser(String name) {
        if (!m_enabled) {
            return new AuthUser(null, null, false, false) {
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
                public boolean authorizeConnector(String connectorName) {
                    return true;
                }
            };
        }
        return m_users.get(name);
    }
}
