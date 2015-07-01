/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.rights;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlNameManager;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.SqlInvariants;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.lib.Collection;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.IntValueHashMap;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.lib.HsqlArrayList;

/**
 * Contains a set of Grantee objects, and supports operations for creating,
 * finding, modifying and deleting Grantee objects for a Database; plus
 * Administrative privileges.
 *
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 *
 * @version 1.9.0
 * @since 1.8.0
 * @see Grantee
 */
public class GranteeManager {

    /**
     * The grantee object for the _SYSTEM role.
     */
    static User systemAuthorisation;

    static {
        HsqlName name = HsqlNameManager.newSystemObjectName(
            SqlInvariants.SYSTEM_AUTHORIZATION_NAME, SchemaObject.GRANTEE);

        systemAuthorisation          = new User(name, null);
        systemAuthorisation.isSystem = true;

        systemAuthorisation.setAdminDirect();
        systemAuthorisation.setInitialSchema(
            SqlInvariants.SYSTEM_SCHEMA_HSQLNAME);

        SqlInvariants.INFORMATION_SCHEMA_HSQLNAME.owner = systemAuthorisation;
        SqlInvariants.SYSTEM_SCHEMA_HSQLNAME.owner      = systemAuthorisation;
    }

    /**
     * Map of grantee-String-to-Grantee-objects.<p>
     * Keys include all USER and ROLE names
     */
    private HashMappedList map = new HashMappedList();

    /**
     * Map of role-Strings-to-Grantee-object.<p>
     * Keys include all ROLES names
     */
    private HashMappedList roleMap = new HashMappedList();

    /**
     * Used only to pass the SchemaManager to Grantees for checking
     * schema authorizations.
     */
    Database database;

    /**
     * The PUBLIC role.
     */
    Grantee publicRole;

    /**
     * The DBA role.
     */
    Grantee dbaRole;

    /**
     * The role for schema creation rights.
     */
    Grantee schemaRole;

    /**
     * The role for changing authorization rights.
     */
    Grantee changeAuthRole;

    /**
     * Construct the GranteeManager for a Database. Construct special Grantee
     * objects for _SYSTEM, PUBLIC and DBA, and add them to the Grantee map.
     *
     * @param database Only needed to link to the RoleManager later on.
     */
    public GranteeManager(Database database) {

        this.database = database;

//        map.add(systemAuthorisation.getNameString(), systemAuthorisation);
//        roleMap.add(systemAuthorisation.getNameString(), systemAuthorisation);
        addRole(
            this.database.nameManager.newHsqlName(
                SqlInvariants.PUBLIC_ROLE_NAME, false, SchemaObject.GRANTEE));

        publicRole          = getRole(SqlInvariants.PUBLIC_ROLE_NAME);
        publicRole.isPublic = true;

        addRole(
            this.database.nameManager.newHsqlName(
                SqlInvariants.DBA_ADMIN_ROLE_NAME, false,
                SchemaObject.GRANTEE));

        dbaRole = getRole(SqlInvariants.DBA_ADMIN_ROLE_NAME);

        dbaRole.setAdminDirect();
        addRole(
            this.database.nameManager.newHsqlName(
                SqlInvariants.SCHEMA_CREATE_ROLE_NAME, false,
                SchemaObject.GRANTEE));

        schemaRole = getRole(SqlInvariants.SCHEMA_CREATE_ROLE_NAME);

        addRole(
            this.database.nameManager.newHsqlName(
                SqlInvariants.CHANGE_AUTH_ROLE_NAME, false,
                SchemaObject.GRANTEE));

        changeAuthRole = getRole(SqlInvariants.CHANGE_AUTH_ROLE_NAME);
    }

    static final IntValueHashMap rightsStringLookup = new IntValueHashMap(7);

    static {
        rightsStringLookup.put(Tokens.T_ALL, GrantConstants.ALL);
        rightsStringLookup.put(Tokens.T_SELECT, GrantConstants.SELECT);
        rightsStringLookup.put(Tokens.T_UPDATE, GrantConstants.UPDATE);
        rightsStringLookup.put(Tokens.T_DELETE, GrantConstants.DELETE);
        rightsStringLookup.put(Tokens.T_INSERT, GrantConstants.INSERT);
        rightsStringLookup.put(Tokens.T_EXECUTE, GrantConstants.EXECUTE);
        rightsStringLookup.put(Tokens.T_USAGE, GrantConstants.USAGE);
        rightsStringLookup.put(Tokens.T_REFERENCES, GrantConstants.REFERENCES);
        rightsStringLookup.put(Tokens.T_TRIGGER, GrantConstants.TRIGGER);
    }

    public Grantee getDBARole() {
        return dbaRole;
    }

    public static Grantee getSystemRole() {
        return systemAuthorisation;
    }

    /**
     * Grants the rights represented by the rights argument on
     * the database object identified by the dbobject argument
     * to the Grantee object identified by name argument.<p>
     *
     *  Note: For the dbobject argument, Java Class objects are identified
     *  using a String object whose value is the fully qualified name
     *  of the Class, while Table and other objects are
     *  identified by an HsqlName object.  A Table
     *  object identifier must be precisely the one obtained by calling
     *  table.getName(); if a different HsqlName
     *  object with an identical name attribute is specified, then
     *  rights checks and tests will fail, since the HsqlName
     *  class implements its {@link HsqlName#hashCode hashCode} and
     *  {@link HsqlName#equals equals} methods based on pure object
     *  identity, rather than on attribute values. <p>
     */
    public void grant(OrderedHashSet granteeList, SchemaObject dbObject,
                      Right right, Grantee grantor, boolean withGrantOption) {

        if (!grantor.isGrantable(dbObject, right)) {
            throw Error.error(ErrorCode.X_0L000, grantor.getNameString());
        }

        if (grantor.isAdmin()) {
            grantor = dbObject.getOwner();
        }

        checkGranteeList(granteeList);

        for (int i = 0; i < granteeList.size(); i++) {
            String  name    = (String) granteeList.get(i);
            Grantee grantee = get(name);

            grantee.grant(dbObject, right, grantor, withGrantOption);

            if (grantee.isRole) {
                updateAllRights(grantee);
            }
        }
    }

    public void checkGranteeList(OrderedHashSet granteeList) {

        for (int i = 0; i < granteeList.size(); i++) {
            String  name    = (String) granteeList.get(i);
            Grantee grantee = get(name);

            if (grantee == null) {
                throw Error.error(ErrorCode.X_28501, name);
            }

            if (isImmutable(name)) {
                throw Error.error(ErrorCode.X_28502, name);
            }
        }
    }

    /**
     * Grant a role to this Grantee.
     */
    public void grant(String granteeName, String roleName, Grantee grantor) {

        Grantee grantee = get(granteeName);

        if (grantee == null) {
            throw Error.error(ErrorCode.X_28501, granteeName);
        }

        if (isImmutable(granteeName)) {
            throw Error.error(ErrorCode.X_28502, granteeName);
        }

        Grantee role = getRole(roleName);

        if (role == null) {
            throw Error.error(ErrorCode.X_0P000, roleName);
        }

        if (role == grantee) {
            throw Error.error(ErrorCode.X_0P501, granteeName);
        }

        // boucherb@users 20050515
        // SQL 2003 Foundation, 4.34.3
        // No cycles of role grants are allowed.
        if (role.hasRole(grantee)) {

            // boucherb@users

            /** @todo: Correct reporting of actual grant path */
            throw Error.error(ErrorCode.X_0P501, roleName);
        }

        if (!grantor.isGrantable(role)) {
            throw Error.error(ErrorCode.X_0L000, grantor.getNameString());
        }

        grantee.grant(role);
        grantee.updateAllRights();

        if (grantee.isRole) {
            updateAllRights(grantee);
        }
    }

    public void checkRoleList(String granteeName, OrderedHashSet roleList,
                              Grantee grantor, boolean grant) {

        Grantee grantee = get(granteeName);

        for (int i = 0; i < roleList.size(); i++) {
            String  roleName = (String) roleList.get(i);
            Grantee role     = getRole(roleName);

            if (role == null) {
                throw Error.error(ErrorCode.X_0P000, roleName);
            }

            if (roleName.equals(SqlInvariants.SYSTEM_AUTHORIZATION_NAME)
                    || roleName.equals(SqlInvariants.PUBLIC_ROLE_NAME)) {
                throw Error.error(ErrorCode.X_28502, roleName);
            }

            if (grant) {
                if (grantee.getDirectRoles().contains(role)) {

                    /** @todo  shouldnt throw */
                    throw Error.error(ErrorCode.X_0P000, granteeName);
                }
            } else {
                if (!grantee.getDirectRoles().contains(role)) {

                    /** @todo  shouldnt throw */
                    throw Error.error(ErrorCode.X_0P000, roleName);
                }
            }

            if (!grantor.isAdmin()) {
                throw Error.error(ErrorCode.X_0L000, grantor.getNameString());
            }
        }
    }

    public void grantSystemToPublic(SchemaObject object, Right right) {
        publicRole.grant(object, right, systemAuthorisation, true);
    }

    /**
     * Revoke a role from a Grantee
     */
    public void revoke(String granteeName, String roleName, Grantee grantor) {

        if (!grantor.isAdmin()) {
            throw Error.error(ErrorCode.X_42507);
        }

        Grantee grantee = get(granteeName);

        if (grantee == null) {
            throw Error.error(ErrorCode.X_28000, granteeName);
        }

        Grantee role = (Grantee) roleMap.get(roleName);

        grantee.revoke(role);
        grantee.updateAllRights();

        if (grantee.isRole) {
            updateAllRights(grantee);
        }
    }

    /**
     * Revokes the rights represented by the rights argument on
     * the database object identified by the dbobject argument
     * from the User object identified by the name
     * argument.<p>
     * @see #grant
     */
    public void revoke(OrderedHashSet granteeList, SchemaObject dbObject,
                       Right rights, Grantee grantor, boolean grantOption,
                       boolean cascade) {

        if (!grantor.isFullyAccessibleByRole(dbObject)) {
            throw Error.error(ErrorCode.X_42501, dbObject.getName().name);
        }

        if (grantor.isAdmin()) {
            grantor = dbObject.getOwner();
        }

        for (int i = 0; i < granteeList.size(); i++) {
            String  name = (String) granteeList.get(i);
            Grantee g    = get(name);

            if (g == null) {
                throw Error.error(ErrorCode.X_28501, name);
            }

            if (isImmutable(name)) {
                throw Error.error(ErrorCode.X_28502, name);
            }
        }

        for (int i = 0; i < granteeList.size(); i++) {
            String  name = (String) granteeList.get(i);
            Grantee g    = get(name);

            g.revoke(dbObject, rights, grantor, grantOption);
            g.updateAllRights();

            if (g.isRole) {
                updateAllRights(g);
            }
        }
    }

    /**
     * Removes a role without any privileges from all grantees
     */
    void removeEmptyRole(Grantee role) {

        for (int i = 0; i < map.size(); i++) {
            Grantee grantee = (Grantee) map.get(i);

            grantee.roles.remove(role);
        }
    }

    /**
     * Removes all rights mappings for the database object identified by
     * the dbobject argument from all Grantee objects in the set.
     */
    public void removeDbObject(HsqlName name) {

        for (int i = 0; i < map.size(); i++) {
            Grantee g = (Grantee) map.get(i);

            g.revokeDbObject(name);
        }
    }

    public void removeDbObjects(OrderedHashSet nameSet) {

        Iterator it = nameSet.iterator();

        while (it.hasNext()) {
            HsqlName name = (HsqlName) it.next();

            for (int i = 0; i < map.size(); i++) {
                Grantee g = (Grantee) map.get(i);

                g.revokeDbObject(name);
            }
        }
    }

    /**
     * First updates all ROLE Grantee objects. Then updates all USER Grantee
     * Objects.
     */
    void updateAllRights(Grantee role) {

        for (int i = 0; i < map.size(); i++) {
            Grantee grantee = (Grantee) map.get(i);

            if (grantee.isRole) {
                grantee.updateNestedRoles(role);
            }
        }

        for (int i = 0; i < map.size(); i++) {
            Grantee grantee = (Grantee) map.get(i);

            if (!grantee.isRole) {
                grantee.updateAllRights();
            }
        }
    }

    /**
     */
    public boolean removeGrantee(String name) {

        /*
         * Explicitly can't remove PUBLIC_USER_NAME and system grantees.
         */
        if (isReserved(name)) {
            return false;
        }

        Grantee g = (Grantee) map.remove(name);

        if (g == null) {
            return false;
        }

        g.clearPrivileges();
        updateAllRights(g);

        if (g.isRole) {
            roleMap.remove(name);
            removeEmptyRole(g);
        }

        return true;
    }

    /**
     * Creates a new Role object under management of this object. <p>
     *
     *  A set of constraints regarding user creation is imposed: <p>
     *
     *  <OL>
     *    <LI>Can't create a role with name same as any right.
     *
     *    <LI>If this object's collection already contains an element whose
     *        name attribute equals the name argument, then
     *        a GRANTEE_ALREADY_EXISTS or ROLE_ALREADY_EXISTS Trace
     *        is thrown.
     *        (This will catch attempts to create Reserved grantee names).
     *  </OL>
     */
    public Grantee addRole(HsqlName name) {

        if (map.containsKey(name.name)) {
            throw Error.error(ErrorCode.X_28503, name.name);
        }

        Grantee g = new Grantee(name, this);

        g.isRole = true;

        map.put(name.name, g);
        roleMap.add(name.name, g);

        return g;
    }

    public User addUser(HsqlName name) {

        if (map.containsKey(name.name)) {
            throw Error.error(ErrorCode.X_28503, name.name);
        }

        User g = new User(name, this);

        map.put(name.name, g);

        return g;
    }

    /**
     * Returns true if named Grantee object exists.
     * This will return true for reserved Grantees
     * SYSTEM_AUTHORIZATION_NAME, ADMIN_ROLE_NAME, PUBLIC_USER_NAME.
     */
    boolean isGrantee(String name) {
        return map.containsKey(name);
    }

    public static int getCheckSingleRight(String right) {

        int r = getRight(right);

        if (r != 0) {
            return r;
        }

        throw Error.error(ErrorCode.X_42581, right);
    }

    /**
     * Translate a string representation or right(s) into its numeric form.
     */
    public static int getRight(String right) {
        return rightsStringLookup.get(right, 0);
    }

    public Grantee get(String name) {
        return (Grantee) map.get(name);
    }

    public Collection getGrantees() {
        return map.values();
    }

    public static boolean validRightString(String rightString) {
        return getRight(rightString) != 0;
    }

    public static boolean isImmutable(String name) {

        return name.equals(SqlInvariants.SYSTEM_AUTHORIZATION_NAME)
               || name.equals(SqlInvariants.DBA_ADMIN_ROLE_NAME)
               || name.equals(SqlInvariants.SCHEMA_CREATE_ROLE_NAME)
               || name.equals(SqlInvariants.CHANGE_AUTH_ROLE_NAME);
    }

    public static boolean isReserved(String name) {

        return name.equals(SqlInvariants.SYSTEM_AUTHORIZATION_NAME)
               || name.equals(SqlInvariants.DBA_ADMIN_ROLE_NAME)
               || name.equals(SqlInvariants.SCHEMA_CREATE_ROLE_NAME)
               || name.equals(SqlInvariants.CHANGE_AUTH_ROLE_NAME)
               || name.equals(SqlInvariants.PUBLIC_ROLE_NAME);
    }

    /**
     * Attempts to drop a Role with the specified name
     *  from this object's set. <p>
     *
     *  A successful drop action consists of: <p>
     *
     *  <UL>
     *
     *    <LI>removing the Grantee object with the specified name
     *        from the set.
     *  </UL> <p>
     *
     */
    public void dropRole(String name) {

        if (!isRole(name)) {
            throw Error.error(ErrorCode.X_0P000, name);
        }

        if (GranteeManager.isReserved(name)) {
            throw Error.error(ErrorCode.X_42507);
        }

        removeGrantee(name);
    }

    public Set getRoleNames() {
        return roleMap.keySet();
    }

    public Collection getRoles() {
        return roleMap.values();
    }

    /**
     * Returns Grantee for the named Role
     */
    public Grantee getRole(String name) {

        Grantee g = (Grantee) roleMap.get(name);

        if (g == null) {
            throw Error.error(ErrorCode.X_0P000, name);
        }

        return g;
    }

    public boolean isRole(String name) {
        return roleMap.containsKey(name);
    }

    public String[] getSQL() {

        HsqlArrayList list = new HsqlArrayList();

        // roles
        Iterator it = getRoles().iterator();

        while (it.hasNext()) {
            Grantee grantee = (Grantee) it.next();

            // ADMIN_ROLE_NAME is not persisted
            if (!GranteeManager.isReserved(grantee.getNameString())) {
                list.add(grantee.getSQL());
            }
        }

        // users
        it = getGrantees().iterator();

        for (; it.hasNext(); ) {
            Grantee grantee = (Grantee) it.next();

            if (grantee instanceof User) {
                list.add(grantee.getSQL());
            }
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public String[] getRightstSQL() {

        HsqlArrayList list     = new HsqlArrayList();
        Iterator      grantees = getGrantees().iterator();

        while (grantees.hasNext()) {
            Grantee grantee = (Grantee) grantees.next();
            String  name    = grantee.getNameString();

            // _SYSTEM user, DBA Role grants not persisted
            if (GranteeManager.isImmutable(name)) {
                continue;
            }

            HsqlArrayList subList = grantee.getRightsSQL();

            list.addAll(subList);
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }
}
