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

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.NumberSequence;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.MultiValueHashMap;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.lib.WrapperIterator;
import org.hsqldb_voltpatches.types.Type;

/**
 * A Grantee Object holds the name, access and administrative rights for a
 * particular grantee.<p>
 * It supplies the methods used to grant, revoke, test
 * and check a grantee's access rights to other database objects.
 * It also holds a reference to the common PUBLIC User Object,
 * which represent the special user refered to in
 * GRANT ... TO PUBLIC statements.<p>
 * The check(), isAccessible() and getGrantedClassNames() methods check the
 * rights granted to the PUBLIC User Object, in addition to individually
 * granted rights, in order to decide which rights exist for the user.
 *
 * Method names ending in Direct indicate methods which do not recurse
 * to look through Roles which "this" object is a member of.
 *
 * We use the word "Admin" (e.g., in private variable "admin" and method
 * "isAdmin()) to mean this Grantee has admin priv by any means.
 * We use the word "adminDirect" (e.g., in private variable "adminDirect"
 * and method "isAdminDirect()) to mean this Grantee has admin priv
 * directly.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 *
 * @version 1.9.0
 * @since 1.8.0
 */
public class Grantee implements SchemaObject {

    boolean isRole;

    /**
     * true if this grantee has database administrator priv directly
     *  (ie., not by membership in any role)
     */
    private boolean isAdminDirect = false;

    /** true if this grantee has database administrator priv by any means. */
    private boolean isAdmin = false;

    /** true if this user can create schemas with its own authorisation */
    boolean isSchemaCreator = false;

    /** true if this grantee is PUBLIC. */
    boolean isPublic = false;

    /** true if this grantee is _SYSTEM. */
    boolean isSystem = false;

    /** Grantee name. */
    protected HsqlName granteeName;

    /** map with database object identifier keys and access privileges values */
    private MultiValueHashMap directRightsMap;

    /** contains righs granted direct, or via roles, expept those of PUBLIC */
    private HashMap fullRightsMap;

    /** These are the DIRECT roles.  Each of these may contain nested roles */
    OrderedHashSet roles;

    /** map with database object identifier keys and access privileges values */
    private MultiValueHashMap grantedRightsMap;

    /** Needed only to give access to the roles for this database */
    protected GranteeManager granteeManager;

    /**  */
    protected Right ownerRights;

    /**
     * Constructor.
     */
    Grantee(HsqlName name, GranteeManager man) {

        fullRightsMap       = new HashMap();
        directRightsMap     = new MultiValueHashMap();
        grantedRightsMap    = new MultiValueHashMap();
        granteeName         = name;
        granteeManager      = man;
        roles               = new OrderedHashSet();
        ownerRights         = new Right();
        ownerRights.isFull  = true;
        ownerRights.grantor = GranteeManager.systemAuthorisation;
        ownerRights.grantee = this;
    }

    public int getType() {
        return SchemaObject.GRANTEE;
    }

    public HsqlName getName() {
        return granteeName;
    }

    public HsqlName getSchemaName() {
        return null;
    }

    public HsqlName getCatalogName() {
        return null;
    }

    public Grantee getOwner() {
        return null;
    }

    public OrderedHashSet getReferences() {
        return null;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_CREATE).append(' ').append(Tokens.T_ROLE);
        sb.append(' ').append(granteeName.statementName);

        return sb.toString();
    }

    public String getNameString() {
        return granteeName.name;
    }

    public String getStatementName() {
        return granteeName.statementName;
    }

    public boolean isRole() {
        return isRole;
    }

    /**
     * Retrieves the map object that represents the rights that have been
     * granted on database objects.  <p>
     *
     * The map has keys and values with the following interpretation: <P>
     *
     * <UL>
     * <LI> The keys are generally (but not limited to) objects having
     *      an attribute or value equal to the name of an actual database
     *      object.
     *
     * <LI> Specifically, the keys act as database object identifiers.
     *
     * <LI> The values are Right objects.
     * </UL>
     */
    public MultiValueHashMap getRights() {

        // necessary to create the script
        return directRightsMap;
    }

    /**
     * Grant a role
     */
    public void grant(Grantee role) {
        roles.add(role);
    }

    /**
     * Revoke a direct role only
     */
    public void revoke(Grantee role) {

        if (!hasRoleDirect(role)) {
            throw Error.error(ErrorCode.X_0P503, role.getNameString());
        }

        roles.remove(role);
    }

    /**
     * Gets direct roles, not roles nested within them.
     */
    public OrderedHashSet getDirectRoles() {
        return roles;
    }

    String getAllRolesAsString() {
        return roleMapToString(getAllRoles());
    }

    public String getDirectRolesAsString() {
        return roleMapToString(roles);
    }

    public String roleMapToString(OrderedHashSet roles) {

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < roles.size(); i++) {
            if (sb.length() > 0) {
                sb.append(',');
            }

            Grantee role = (Grantee) roles.get(i);

            sb.append(role.getNameString());
        }

        return sb.toString();
    }

    /**
     * Gets direct and indirect roles.
     */
    public OrderedHashSet getAllRoles() {

        OrderedHashSet set = getGranteeAndAllRoles();

        // Since we added "Grantee" in addition to Roles, need to remove self.
        set.remove(this);

        return set;
    }

    public OrderedHashSet getGranteeAndAllRoles() {

        OrderedHashSet set = new OrderedHashSet();

        addGranteeAndRoles(set);

        return set;
    }

    public OrderedHashSet getGranteeAndAllRolesWithPublic() {

        OrderedHashSet set = new OrderedHashSet();

        addGranteeAndRoles(set);
        set.add(granteeManager.publicRole);

        return set;
    }

    /**
     * Adds to given Set this.sName plus all roles and nested roles.
     *
     * @return Given role with new elements added.
     */
    private OrderedHashSet addGranteeAndRoles(OrderedHashSet set) {

        Grantee candidateRole;

        set.add(this);

        for (int i = 0; i < roles.size(); i++) {
            candidateRole = (Grantee) roles.get(i);

            if (!set.contains(candidateRole)) {
                candidateRole.addGranteeAndRoles(set);
            }
        }

        return set;
    }

    /**
     * returns a map with grantee name keys and sets of granted roles as value
     */
    public void addAllRoles(HashMap map) {

        for (int i = 0; i < roles.size(); i++) {
            Grantee role = (Grantee) roles.get(i);

            map.put(role.granteeName.name, role.roles);
        }
    }

    public boolean hasRoleDirect(Grantee role) {
        return roles.contains(role);
    }

    public boolean hasRole(Grantee role) {
        return getAllRoles().contains(role);
    }

    /**
     * Grants the specified rights on the specified database object. <p>
     *
     * Keys stored in rightsMap for database tables are their HsqlName
     * attribute. This allows rights to persist when a table is renamed. <p>
     */
    void grant(SchemaObject object, Right right, Grantee grantor,
               boolean withGrant) {

        final HsqlName name            = object.getName();
        final Right    grantableRights = grantor.getAllGrantableRights(object);
        Right          existingRight   = null;

        if (right == Right.fullRights) {
            if (grantableRights.isEmpty()) {
                return;    // has no rights
            }

            right = grantableRights;
        } else {
            if (!grantableRights.contains(right)) {
                throw Error.error(ErrorCode.X_0L000);
            }
        }

        Iterator it = directRightsMap.get(name);

        while (it.hasNext()) {
            Right existing = (Right) it.next();

            if (existing.grantor == grantor) {
                existingRight = existing;

                existingRight.add(right);

                break;
            }
        }

        if (existingRight == null) {
            existingRight         = right.duplicate();
            existingRight.grantor = grantor;
            existingRight.grantee = this;

            directRightsMap.put(name, existingRight);
        }

        if (withGrant) {
            if (existingRight.grantableRights == null) {
                existingRight.grantableRights = right.duplicate();
            } else {
                existingRight.grantableRights.add(right);
            }
        }

        if (!grantor.isSystem) {

            // based on assumption that there is no need to access
            grantor.grantedRightsMap.put(name, existingRight);
        }

        updateAllRights();
    }

    /**
     * Revokes the specified rights on the specified database object. <p>
     *
     * If, after removing the specified rights, no rights remain on the
     * database object, then the key/value pair for that object is removed
     * from the rights map
     */
    void revoke(SchemaObject object, Right right, Grantee grantor,
                boolean grantOption) {

        final HsqlName name     = object.getName();
        Iterator       it       = directRightsMap.get(name);
        Right          existing = null;

        while (it.hasNext()) {
            existing = (Right) it.next();

            if (existing.grantor == grantor) {
                break;
            }
        }

        if (existing == null) {
            return;
        }

        if (existing.grantableRights != null) {
            existing.grantableRights.remove(object, right);
        }

        if (grantOption) {
            return;
        }

        if (right.isFull) {
            directRightsMap.remove(name, existing);
            grantor.grantedRightsMap.remove(name, existing);
            updateAllRights();

            return;
        }

        existing.remove(object, right);

        if (existing.isEmpty()) {
            directRightsMap.remove(name, existing);
            grantor.grantedRightsMap.remove(object, existing);
        }

        updateAllRights();

        return;
    }

    /**
     * Revokes all rights on the specified database object.<p>
     *
     * This method removes any existing mapping from the rights map
     */
    void revokeDbObject(HsqlName name) {

        directRightsMap.remove(name);
        grantedRightsMap.remove(name);
        fullRightsMap.remove(name);
    }

    /**
     * Revokes all rights from this Grantee object.  The map is cleared and
     * the database administrator role attribute is set false.
     */
    void clearPrivileges() {

        roles.clear();
        directRightsMap.clear();
        grantedRightsMap.clear();
        fullRightsMap.clear();

        isAdmin = false;
    }

    public OrderedHashSet getColumnsForAllPrivileges(Table table) {

        if (isFullyAccessibleByRole(table)) {
            return table.getColumnNameSet();
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        return right == null ? Right.emptySet
                             : right.getColumnsForAllRights(table);
    }

    public OrderedHashSet getAllDirectPrivileges(SchemaObject object) {

        if (object.getOwner() == this) {
            OrderedHashSet set = new OrderedHashSet();

            set.add(ownerRights);

            return set;
        }

        Iterator rights = directRightsMap.get(object.getName());

        if (rights.hasNext()) {
            OrderedHashSet set = new OrderedHashSet();

            while (rights.hasNext()) {
                set.add(rights.next());
            }

            return set;
        }

        return Right.emptySet;
    }

    public OrderedHashSet getAllGrantedPrivileges(SchemaObject object) {

        Iterator rights = grantedRightsMap.get(object.getName());

        if (rights.hasNext()) {
            OrderedHashSet set = new OrderedHashSet();

            while (rights.hasNext()) {
                set.add(rights.next());
            }

            return set;
        }

        return Right.emptySet;
    }

    /**
     * Checks if a right represented by the methods
     * have been granted on the specified database object. <p>
     *
     * This is done by checking that a mapping exists in the rights map
     * from the dbobject argument. Otherwise, it throws.
     */
    public void checkSelect(Table table, boolean[] checkList) {

        if (isFullyAccessibleByRole(table)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right != null && right.canSelect(table, checkList)) {
            return;
        }

        throw Error.error(ErrorCode.X_42501, table.getName().name);
    }

    public void checkInsert(Table table, boolean[] checkList) {

        if (isFullyAccessibleByRole(table)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right != null && right.canInsert(table, checkList)) {
            return;
        }

        throw Error.error(ErrorCode.X_42501, table.getName().name);
    }

    public void checkUpdate(Table table, boolean[] checkList) {

        if (isFullyAccessibleByRole(table)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right != null && right.canUpdate(table, checkList)) {
            return;
        }

        throw Error.error(ErrorCode.X_42501, table.getName().name);
    }

    public void checkReferences(Table table, boolean[] checkList) {

        if (isFullyAccessibleByRole(table)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right != null && right.canReference(table, checkList)) {
            return;
        }

        throw Error.error(ErrorCode.X_42501, table.getName().name);
    }

    public void checkTrigger(Table table, boolean[] checkList) {

        if (isFullyAccessibleByRole(table)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right != null && right.canReference(table, checkList)) {
            return;
        }

        throw Error.error(ErrorCode.X_42501, table.getName().name);
    }

    public void checkDelete(Table table) {

        if (isFullyAccessibleByRole(table)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right != null && right.canDelete()) {
            return;
        }

        throw Error.error(ErrorCode.X_42501, table.getName().name);
    }

    public void checkAccess(SchemaObject object) {

        if (isFullyAccessibleByRole(object)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(object.getName());

        if (right != null && !right.isEmpty()) {
            return;
        }

        throw Error.error(ErrorCode.X_42501, object.getName().name);
    }

    /**
     * Checks if this object can modify schema objects or grant access rights
     * to them.
     */
    public void checkSchemaUpdateOrGrantRights(String schemaName) {

        if (!hasSchemaUpdateOrGrantRights(schemaName)) {
            throw Error.error(ErrorCode.X_42501, schemaName);
        }
    }

    /**
     * Checks if this object can modify schema objects or grant access rights
     * to them.
     */
    public boolean hasSchemaUpdateOrGrantRights(String schemaName) {

        // If a DBA
        if (isAdmin()) {
            return true;
        }

        Grantee schemaOwner =
            granteeManager.database.schemaManager.toSchemaOwner(schemaName);

        // If owner of Schema
        if (schemaOwner == this) {
            return true;
        }

        // If a member of Schema authorization role
        if (hasRole(schemaOwner)) {
            return true;
        }

        return false;
    }

    public boolean isGrantable(SchemaObject object, Right right) {

        if (isFullyAccessibleByRole(object)) {
            return true;
        }

        Right grantableRights = getAllGrantableRights(object);

        return grantableRights.contains(right);
    }

    public boolean isGrantable(Grantee role) {
        return isAdmin;
    }

    public boolean isFullyAccessibleByRole(SchemaObject object) {
        return isFullyAccessibleByRole(object.getName());
    }

    public boolean isFullyAccessibleByRole(HsqlName name) {

        if (isAdmin) {
            return true;
        }

        if (name.schema == null) {
            return false;
        }

        Grantee owner = name.schema.owner;

        if (owner == this) {
            return true;
        }

        if (hasRole(owner)) {
            return true;
        }

        return false;
    }

    /**
     * Checks whether this Grantee has administrative privs either directly
     * or indirectly. Otherwise it throws.
     */
    public void checkAdmin() {

        if (!isAdmin()) {
            throw Error.error(ErrorCode.X_42507);
        }
    }

    /**
     * Returns true if this Grantee has administrative privs either directly
     * or indirectly.
     */
    public boolean isAdmin() {
        return isAdmin;
    }

    /**
     * Returns true if this Grantee can create schemas with own authorization.
     */
    public boolean isSchemaCreator() {
        return isAdmin || hasRole(granteeManager.schemaRole);
    }

    /**
     * Returns true if this Grantee can change to a different user.
     */
    public boolean canChangeAuthorisation() {
        return isAdmin || hasRole(granteeManager.changeAuthRole);
    }

    /**
     * Returns true if this grantee object is for the PUBLIC role.
     */
    public boolean isPublic() {
        return isPublic;
    }

    /**
     * Violates naming convention (for backward compatibility).
     * Should be "setAdminDirect(boolean").
     */
    void setAdminDirect() {
        isAdmin = isAdminDirect = true;
    }

    /**
     * Recursive method used with ROLE Grantee objects to set the fullRightsMap
     * and admin flag for all the roles.
     *
     * If a new ROLE is granted to a ROLE Grantee object, the ROLE should first
     * be added to the Set of ROLE Grantee objects (roles) for the grantee.
     * The grantee will be the parameter.
     *
     * If the direct permissions granted to an existing ROLE Grentee is
     * modified no extra initial action is necessary.
     * The existing Grantee will be the parameter.
     *
     * If an existing ROLE is REVOKEed from a ROLE, it should first be removed
     * from the set of ROLE Grantee objects in the containing ROLE.
     * The containing ROLE will be the parameter.
     *
     * If an existing ROLE is DROPped, all its privileges should be cleared
     * first. The ROLE will be the parameter. After calling this method on
     * all other roles, the DROPped role should be removed from all grantees.
     *
     * After the initial modification, this method should be called iteratively
     * on all the ROLE Grantee objects contained in RoleManager.
     *
     * The updateAllRights() method is then called iteratively on all the
     * USER Grantee objects contained in UserManager.
     * @param role a modified, revoked or dropped role.
     * @return true if this Grantee has possibly changed as a result
     */
    boolean updateNestedRoles(Grantee role) {

        boolean hasNested = false;

        if (role != this) {
            for (int i = 0; i < roles.size(); i++) {
                Grantee currentRole = (Grantee) roles.get(i);

                hasNested |= currentRole.updateNestedRoles(role);
            }
        }

        if (hasNested) {
            updateAllRights();
        }

        return hasNested || role == this;
    }

    /**
     * Method used with all Grantee objects to set the full set of rights
     * according to those inherited form ROLE Grantee objects and those
     * granted to the object itself.
     */

    /**
     * @todo -- see if this is correct and the currentRole.fullRightsMap
     * is always updated prior to being added to this.fullRightsMap
     */
    void updateAllRights() {

        fullRightsMap.clear();

        isAdmin = isAdminDirect;

        for (int i = 0; i < roles.size(); i++) {
            Grantee currentRole = (Grantee) roles.get(i);

            addToFullRights(currentRole.fullRightsMap);

            isAdmin |= currentRole.isAdmin();
        }

        addToFullRights(directRightsMap);

        if (!isRole && !isPublic && !isSystem) {
            addToFullRights(granteeManager.publicRole.fullRightsMap);
        }
    }

    /**
     * Full or partial rights are added to existing
     */
    void addToFullRights(HashMap map) {

        Iterator it = map.keySet().iterator();

        while (it.hasNext()) {
            Object key      = it.next();
            Right  add      = (Right) map.get(key);
            Right  existing = (Right) fullRightsMap.get(key);

            if (existing == null) {
                existing = add.duplicate();

                fullRightsMap.put(key, existing);
            } else {
                existing.add(add);
            }

            if (add.grantableRights == null) {
                continue;
            }

            if (existing.grantableRights == null) {
                existing.grantableRights = add.grantableRights.duplicate();
            } else {
                existing.grantableRights.add(add.grantableRights);
            }
        }
    }

    /**
     * Full or partial rights are added to existing
     */
    void addToFullRights(MultiValueHashMap map) {

        Iterator it = map.keySet().iterator();

        while (it.hasNext()) {
            Object   key      = it.next();
            Iterator values   = map.get(key);
            Right    existing = (Right) fullRightsMap.get(key);

            while (values.hasNext()) {
                Right add = (Right) values.next();

                if (existing == null) {
                    existing = add.duplicate();

                    fullRightsMap.put(key, existing);
                } else {
                    existing.add(add);
                }

                if (add.grantableRights == null) {
                    continue;
                }

                if (existing.grantableRights == null) {
                    existing.grantableRights = add.grantableRights.duplicate();
                } else {
                    existing.grantableRights.add(add.grantableRights);
                }
            }
        }
    }

    /**
     * Iteration of all visible grantees, including self. <p>
     *
     * For grantees with admin, this is all grantees.
     * For regular grantees, this is self plus all roles granted directly
     * or indirectly
     */
    public Set visibleGrantees() {

        HashSet        grantees = new HashSet();
        GranteeManager gm       = granteeManager;

        if (isAdmin()) {
            grantees.addAll(gm.getGrantees());
        } else {
            grantees.add(this);

            Iterator it = getAllRoles().iterator();

            while (it.hasNext()) {
                grantees.add(it.next());
            }
        }

        return grantees;
    }

    /**
     * Set of all non-reserved visible grantees, including self. <p>
     *
     * For grantees with admin, this is all grantees.
     * For regular grantees, this is self plus all roles granted directly
     * or indirectly. <P>
     *
     * @param andPublic when <tt>true</tt> retains the reserved PUBLIC grantee
     */
    public Set nonReservedVisibleGrantees(boolean andPublic) {

        Set            grantees = visibleGrantees();
        GranteeManager gm       = granteeManager;

        grantees.remove(gm.dbaRole);
        grantees.remove(GranteeManager.systemAuthorisation);

        if (!andPublic) {
            grantees.remove(gm.publicRole);
        }

        return grantees;
    }

    public boolean hasNonSelectTableRight(Table table) {

        if (isFullyAccessibleByRole(table)) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right == null) {
            return false;
        }

        return right.isFull || right.isFullDelete || right.isFullInsert
               || right.isFullUpdate || right.isFullReferences
               || right.isFullTrigger;
    }

    public boolean hasTableRight(Table table) {

        if (isFullyAccessibleByRole(table)) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right == null) {
            return false;
        }

        return right.isFull || right.isFullDelete || right.isFullInsert
               || right.isFullUpdate || right.isFullReferences
               || right.isFullTrigger || right.isFullSelect;
    }

    public Iterator getAllDirectFullRights(SchemaObject object) {

        Grantee owner = object.getOwner();

        if (owner == this) {
            return new WrapperIterator(ownerRights);
        }

        return directRightsMap.get(object.getName());
    }

    public Right getAllGrantableRights(SchemaObject object) {

        if (isAdmin) {
            return object.getOwner().ownerRights;
        }

        if (object.getOwner() == this) {
            return ownerRights;
        }

        if (roles.contains(object.getOwner())) {
            return object.getOwner().ownerRights;
        }

        OrderedHashSet set = getAllRoles();

        for (int i = 0; i < set.size(); i++) {
            Grantee role = (Grantee) set.get(i);

            if (object.getOwner() == role) {
                return role.ownerRights;
            }
        }

        Right right = (Right) fullRightsMap.get(object.getName());

        return right == null || right.grantableRights == null ? Right.noRights
                                                              : right
                                                              .grantableRights;
    }

    public boolean isAccessible(SchemaObject object, int privilegeType) {

        if (isFullyAccessibleByRole(object)) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(object.getName());

        if (right == null) {
            return false;
        }

        return right.canAccess(object, privilegeType);
    }

    /**
     * returns true if grantee has any privilege (to any column) of the object
     */
    public boolean isAccessible(SchemaObject object) {
        return isAccessible(object.getName());
    }

    public boolean isAccessible(HsqlName name) {

        if (isFullyAccessibleByRole(name)) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(name);

        if (right != null && !right.isEmpty()) {
            return true;
        }

        if (!isPublic) {
            return granteeManager.publicRole.isAccessible(name);
        }

        return false;
    }

    public HsqlArrayList getRightsSQL() {

        HsqlArrayList list       = new HsqlArrayList();
        String        roleString = getDirectRolesAsString();

        if (roleString.length() != 0) {
            list.add("GRANT " + roleString + " TO " + getStatementName());
        }

        MultiValueHashMap rightsMap = getRights();
        Iterator          dbObjects = rightsMap.keySet().iterator();

        while (dbObjects.hasNext()) {
            Object   nameObject = dbObjects.next();
            Iterator rights     = rightsMap.get(nameObject);

            while (rights.hasNext()) {
                Right        right    = (Right) rights.next();
                StringBuffer sb       = new StringBuffer(128);
                HsqlName     hsqlname = (HsqlName) nameObject;

                switch (hsqlname.type) {

                    case SchemaObject.TABLE :
                    case SchemaObject.VIEW :
                        Table table =
                            granteeManager.database.schemaManager
                                .findUserTable(null, hsqlname.name,
                                               hsqlname.schema.name);

                        if (table != null) {
                            sb.append(Tokens.T_GRANT).append(' ');
                            sb.append(right.getTableRightsSQL(table));
                            sb.append(' ').append(Tokens.T_ON).append(' ');
                            sb.append("TABLE ").append(
                                hsqlname.getSchemaQualifiedStatementName());
                        }
                        break;

                    case SchemaObject.SEQUENCE :
                        NumberSequence sequence =
                            (NumberSequence) granteeManager.database
                                .schemaManager
                                .findSchemaObject(hsqlname.name,
                                                  hsqlname.schema.name,
                                                  SchemaObject.SEQUENCE);

                        if (sequence != null) {
                            sb.append(Tokens.T_GRANT).append(' ');
                            sb.append(Tokens.T_USAGE);
                            sb.append(' ').append(Tokens.T_ON).append(' ');
                            sb.append("SEQUENCE ").append(
                                hsqlname.getSchemaQualifiedStatementName());
                        }
                        break;

                    case SchemaObject.DOMAIN :
                        Type domain =
                            (Type) granteeManager.database.schemaManager
                                .findSchemaObject(hsqlname.name,
                                                  hsqlname.schema.name,
                                                  SchemaObject.DOMAIN);

                        if (domain != null) {
                            sb.append(Tokens.T_GRANT).append(' ');
                            sb.append(Tokens.T_USAGE);
                            sb.append(' ').append(Tokens.T_ON).append(' ');
                            sb.append("DOMAIN ").append(
                                hsqlname.getSchemaQualifiedStatementName());
                        }
                        break;

                    case SchemaObject.TYPE :
                        Type type =
                            (Type) granteeManager.database.schemaManager
                                .findSchemaObject(hsqlname.name,
                                                  hsqlname.schema.name,
                                                  SchemaObject.DOMAIN);

                        if (type != null) {
                            sb.append(Tokens.T_GRANT).append(' ');
                            sb.append(Tokens.T_USAGE);
                            sb.append(' ').append(Tokens.T_ON).append(' ');
                            sb.append("TYPE ").append(
                                hsqlname.getSchemaQualifiedStatementName());
                        }
                        break;
                }

                if (sb.length() == 0) {
                    continue;
                }

                sb.append(' ').append(Tokens.T_TO).append(' ');
                sb.append(getStatementName());
                list.add(sb.toString());
            }
        }

        return list;
    }
}
