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
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.lib.StringConverter;

/**
 * A User Object extends Grantee with password for a
 * particular database user.<p>
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 *
 * @version 1.9.0
 * @since 1.8.0
 */
public class User extends Grantee {

    /** password. */
    private String password;

    /** default schema when new Sessions started (defaults to PUBLIC schema) */
    private HsqlName initialSchema = null;

    /**
     * Constructor
     */
    User(HsqlName name, GranteeManager manager) {
        super(name, manager);

        if (manager != null) {
            updateAllRights();
        }
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_CREATE).append(' ').append(Tokens.T_USER);
        sb.append(' ').append(granteeName.statementName).append(' ');
        sb.append(Tokens.T_PASSWORD).append(' ');
        sb.append('\'').append(password).append('\'');

        return sb.toString();
    }

    public void setPassword(String password) {

        /** @todo - introduce complexity interface */
        // checkComplexity(password);
        // requires: UserManager.createSAUser(), UserManager.createPublicUser()
        this.password = password;
    }

    /**
     * Checks if this object's password attibute equals
     * specified argument, else throws.
     */
    public void checkPassword(String value) {

        if (!value.equals(password)) {
            throw Error.error(ErrorCode.X_28000);
        }
    }

    /**
     * Returns the initial schema for the user
     */
    public HsqlName getInitialSchema() {
        return initialSchema;
    }

    public HsqlName getInitialOrDefaultSchema() {

        if (initialSchema != null) {
            return initialSchema;
        }

        HsqlName schema =
            granteeManager.database.schemaManager.findSchemaHsqlName(
                getNameString());

        if (schema == null) {
            return granteeManager.database.schemaManager
                .getDefaultSchemaHsqlName();
        } else {
            return schema;
        }
    }

    /**
     * This class does not have access to the SchemaManager, therefore
     * caller should verify that the given schemaName exists.
     *
     * @param schema An existing schema.  Null value allowed,
     *                   which means use the DB default session schema.
     */
    public void setInitialSchema(HsqlName schema) {
        initialSchema = schema;
    }

    /**
     * Returns the ALTER USER DDL character sequence that preserves the
     * this user's current password value and mode. <p>
     *
     * @return  the DDL
     */
    public String getAlterUserSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_ALTER).append(' ');
        sb.append(Tokens.T_USER).append(' ');
        sb.append(getStatementName()).append(' ');
        sb.append(Tokens.T_SET).append(' ');
        sb.append(Tokens.T_PASSWORD).append(' ');
        sb.append('"').append(password).append('"');

        return sb.toString();
    }

    public String getInitialSchemaSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_ALTER).append(' ');
        sb.append(Tokens.T_USER).append(' ');
        sb.append(getStatementName()).append(' ');
        sb.append(Tokens.T_SET).append(' ');
        sb.append(Tokens.T_INITIAL).append(' ');
        sb.append(Tokens.T_SCHEMA).append(' ');
        sb.append(initialSchema.statementName);

        return sb.toString();
    }

    /**
     * Returns the DDL string
     * sequence that creates this user.
     *
     */
    public String getCreateUserSQL() {

        StringBuffer sb = new StringBuffer(64);

        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_USER).append(' ');
        sb.append(getStatementName()).append(' ');
        sb.append(Tokens.T_PASSWORD).append(' ');
        sb.append(StringConverter.toQuotedString(password, '"', true));

        return sb.toString();
    }

    /**
     * Retrieves the redo log character sequence for connecting
     * this user
     *
     * @return the redo log character sequence for connecting
     *      this user
     */
    public String getConnectUserSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_SET).append(' ');
        sb.append(Tokens.T_SESSION).append(' ');
        sb.append(Tokens.T_AUTHORIZATION).append(' ');
        sb.append(StringConverter.toQuotedString(getNameString(), '\'', true));

        return sb.toString();
    }
}
