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


package org.hsqldb_voltpatches.types;

import org.hsqldb_voltpatches.Constraint;
import org.hsqldb_voltpatches.Expression;
import org.hsqldb_voltpatches.HsqlNameManager;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.rights.Grantee;

/**
 * Class for DOMAIN and DISTINCT objects.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class UserTypeModifier {

    final HsqlName name;
    final int      schemaObjectType;
    final Type     dataType;
    Constraint[]   constraints = Constraint.emptyArray;
    Expression     defaultExpression;
    boolean        isNullable = true;

    public UserTypeModifier(HsqlName name, int type, Type dataType) {

        this.name             = name;
        this.schemaObjectType = type;
        this.dataType         = dataType;
    }

    public int schemaObjectType() {
        return schemaObjectType;
    }

    public void addConstraint(Constraint c) {

        int position = constraints.length;

        constraints = (Constraint[]) ArrayUtil.resizeArray(constraints,
                position + 1);
        constraints[position] = c;

        setNotNull();
    }

    public void removeConstraint(String name) {

        for (int i = 0; i < constraints.length; i++) {
            if (constraints[i].getName().name.equals(name)) {
                constraints =
                    (Constraint[]) ArrayUtil.toAdjustedArray(constraints,
                        null, i, -1);

                break;
            }
        }

        setNotNull();
    }

    public Constraint getConstraint(String name) {

        for (int i = 0; i < constraints.length; i++) {
            if (constraints[i].getName().name.equals(name)) {
                return constraints[i];
            }
        }

        return null;
    }

    public Constraint[] getConstraints() {
        return constraints;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public Expression getDefaultClause() {
        return defaultExpression;
    }

    public void setDefaultClause(Expression defaultExpression) {
        this.defaultExpression = defaultExpression;
    }

    public void removeDefaultClause() {
        defaultExpression = null;
    }

    private void setNotNull() {

        isNullable = true;

        for (int i = 0; i < constraints.length; i++) {
            if (constraints[i].isNotNull()) {
                isNullable = false;
            }
        }
    }

    // interface specific methods
    public int getType() {
        return schemaObjectType;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {

        if (constraints.length == 0) {
            return null;
        }

        OrderedHashSet set = new OrderedHashSet();

        for (int i = 0; i < constraints.length; i++) {
            OrderedHashSet subSet = constraints[i].getReferences();

            if (subSet != null) {
                set.addAll(subSet);
            }
        }

        return set;
    }

    public final OrderedHashSet getComponents() {

        if (constraints == null) {
            return null;
        }

        OrderedHashSet set = new OrderedHashSet();

        set.addAll(constraints);

        return set;
    }

    public void compile(Session session) {

        for (int i = 0; i < constraints.length; i++) {
            constraints[i].compile(session);
        }
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        if (schemaObjectType == SchemaObject.TYPE) {
            sb.append(Tokens.T_CREATE).append(' ').append(
                Tokens.T_TYPE).append(' ');
            sb.append(name.getSchemaQualifiedStatementName());
            sb.append(' ').append(Tokens.T_AS).append(' ');
            sb.append(dataType.getDefinition());
        } else {
            sb.append(Tokens.T_CREATE).append(' ').append(
                Tokens.T_DOMAIN).append(' ');
            sb.append(name.getSchemaQualifiedStatementName());
            sb.append(' ').append(Tokens.T_AS).append(' ');
            sb.append(dataType.getDefinition());

            if (defaultExpression != null) {
                sb.append(' ').append(Tokens.T_DEFAULT).append(' ');
                sb.append(defaultExpression.getSQL());
            }

            for (int i = 0; i < constraints.length; i++) {
                sb.append(' ').append(Tokens.T_CONSTRAINT).append(' ');
                sb.append(constraints[i].getName().statementName).append(' ');
                sb.append(Tokens.T_CHECK).append('(').append(
                    constraints[i].getCheckSQL()).append(')');
            }
        }

        return sb.toString();
    }
}
