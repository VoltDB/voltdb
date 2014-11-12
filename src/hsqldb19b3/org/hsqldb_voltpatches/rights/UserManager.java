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
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HsqlArrayList;

/**
 * Manages the User objects for a Database instance.
 * The special users PUBLIC_USER_NAME and SYSTEM_AUTHORIZATION_NAME
 * are created and managed here.  SYSTEM_AUTHORIZATION_NAME is also
 * special in that the name is not kept in the user "list"
 * (PUBLIC_USER_NAME is kept in the list because it's needed by MetaData
 * routines via "listVisibleUsers(x, true)").
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 1.8.0
 * @since 1.7.2
 * @see  User
 */
public final class UserManager {

    /**
     * This object's set of User objects. <p>
     *
     * Note: The special _SYSTEM  role
     * is not included in this list but the special PUBLIC
     * User object is kept in the list because it's needed by MetaData
     * routines via "listVisibleUsers(x, true)".
     */
    private HashMappedList userList;
    private GranteeManager granteeManager;

    /**
     * Construction happens once for each Database instance.
     *
     * Creates special users PUBLIC_USER_NAME and SYSTEM_AUTHORIZATION_NAME.
     * Sets up association with the GranteeManager for this database.
     */
    public UserManager(Database database) {
        granteeManager = database.getGranteeManager();
        userList       = new HashMappedList();
    }

    /**
     * Creates a new User object under management of this object. <p>
     *
     *  A set of constraints regarding user creation is imposed: <p>
     *
     *  <OL>
     *    <LI>If the specified name is null, then an
     *        ASSERTION_FAILED exception is thrown stating that
     *        the name is null.
     *
     *    <LI>If this object's collection already contains an element whose
     *        name attribute equals the name argument, then
     *        a GRANTEE_ALREADY_EXISTS exception is thrown.
     *        (This will catch attempts to create Reserved grantee names).
     *  </OL>
     */
    public User createUser(HsqlName name,
                           String password) {

        // This will throw an appropriate exception if grantee already exists,
        // regardless of whether the name is in any User, Role, etc. list.
        User user = granteeManager.addUser(name);

        user.setPassword(password);

        boolean success = userList.add(name.name, user);

        if (!success) {
            throw Error.error(ErrorCode.X_28503, name.statementName);
        }

        return user;
    }

    /**
     * Attempts to drop a User object with the specified name
     *  from this object's set. <p>
     *
     *  A successful drop action consists of: <p>
     *
     *  <UL>
     *
     *    <LI>removing the User object with the specified name
     *        from the set.
     *
     *    <LI>revoking all rights from the removed User<br>
     *        (this ensures that in case there are still references to the
     *        just dropped User object, those references
     *        cannot be used to erronously access database objects).
     *
     *  </UL> <p>
     *
     */
    public void dropUser(String name) {

        boolean reservedUser = GranteeManager.isReserved(name);

        if (reservedUser) {
            throw Error.error(ErrorCode.X_28502, name);
        }

        boolean result = granteeManager.removeGrantee(name);

        if (!result) {
            throw Error.error(ErrorCode.X_28501, name);
        }

        User user = (User) userList.remove(name);

        if (user == null) {
            throw Error.error(ErrorCode.X_28501, name);
        }
    }

    /**
     * Returns the User object with the specified name and
     * password from this object's set.
     */
    public User getUser(String name, String password) {

        if (name == null) {
            name = "";
        }

        if (password == null) {
            password = "";
        }

        User user = get(name);

        user.checkPassword(password);

        return user;
    }

    /**
     * Retrieves this object's set of User objects as
     *  an associative list.
     */
    public HashMappedList getUsers() {
        return userList;
    }

    public boolean exists(String name) {
        return userList.get(name) == null ? false
                                          : true;
    }

    /**
     * Returns the User object identified by the
     * name argument.
     */
    public User get(String name) {

        User user = (User) userList.get(name);

        if (user == null) {
            throw Error.error(ErrorCode.X_28501, name);
        }

        return user;
    }

    /**
     * Retrieves the <code>User</code> objects representing the database
     * users that are visible to the <code>User</code> object
     * represented by the <code>session</code> argument. <p>
     *
     * If the <code>session</code> argument's <code>User</code> object
     * attribute has isAdmin() true (directly or by virtue of a Role),
     * then all of the
     * <code>User</code> objects in this collection are considered visible.
     * Otherwise, only this object's special <code>PUBLIC</code>
     * <code>User</code> object attribute and the session <code>User</code>
     * object, if it exists in this collection, are considered visible. <p>
     *
     * @param session The <code>Session</code> object used to determine
     *          visibility
     * @return a list of <code>User</code> objects visible to
     *          the <code>User</code> object contained by the
     *         <code>session</code> argument.
     *
     */
    public HsqlArrayList listVisibleUsers(Session session) {

        HsqlArrayList list;
        User          user;
        boolean       isAdmin;
        String        sessionName;
        String        userName;

        list        = new HsqlArrayList();
        isAdmin     = session.isAdmin();
        sessionName = session.getUsername();

        if (userList == null || userList.size() == 0) {
            return list;
        }

        for (int i = 0; i < userList.size(); i++) {
            user = (User) userList.get(i);

            if (user == null) {
                continue;
            }

            userName = user.getNameString();

            if (isAdmin) {
                list.add(user);
            } else if (sessionName.equals(userName)) {
                list.add(user);
            }
        }

        return list;
    }

    /**
     * Returns the specially constructed
     * <code>SYSTEM_AUTHORIZATION_NAME</code>
     * <code>User</code> object for the current <code>Database</code> object.
     *
     * @return the <code>SYS_AUTHORIZATION_NAME</code>
     *          <code>User</code> object
     *
     */
    public User getSysUser() {
        return GranteeManager.systemAuthorisation;
    }

    public synchronized void removeSchemaReference(String schemaName) {

        for (int i = 0; i < userList.size(); i++) {
            User     user   = (User) userList.get(i);
            HsqlName schema = user.getInitialSchema();

            if (schema == null) {
                continue;
            }

            if (schemaName.equals(schema.name)) {
                user.setInitialSchema(null);
            }
        }
    }

    public String[] getInitialSchemaSQL() {

        HsqlArrayList list = new HsqlArrayList(userList.size());

        for (int i = 0; i < userList.size(); i++) {
            User user = (User) userList.get(i);

            if (user.isSystem) {
                continue;
            }

            HsqlName name = user.getInitialSchema();

            if (name == null) {
                continue;
            }

            list.add(user.getInitialSchemaSQL());
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }
}
