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


package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of SQL table column metadata.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class ColumnSchema extends ColumnBase implements SchemaObject {

    public final static ColumnSchema[] emptyArray = new ColumnSchema[]{};

    //
    private HsqlName       columnName;
    private boolean        isPrimaryKey;
    private Expression     defaultExpression;
    private Expression     generatingExpression;
    private NumberSequence sequence;

    /**
     * Creates a column defined in DDL statement.
     */
    public ColumnSchema(HsqlName name, Type type, boolean isNullable,
                        boolean isPrimaryKey, Expression defaultExpression) {

        columnName             = name;
        nullability = isNullable ? SchemaObject.Nullability.NULLABLE
                                 : SchemaObject.Nullability.NO_NULLS;
        this.dataType          = type;
        this.isPrimaryKey      = isPrimaryKey;
        this.defaultExpression = defaultExpression;
    }

    public int getType() {
        return columnName.type;
    }

    public HsqlName getName() {
        return columnName;
    }

    @Override
    public String getNameString() {
        return columnName.name;
    }

    @Override
    public String getTableNameString() {
        return columnName.parent == null ? null
                                         : columnName.parent.name;
    }

    public HsqlName getSchemaName() {
        return columnName.schema;
    }

    @Override
    public String getSchemaNameString() {
        return columnName.schema == null ? null
                                         : columnName.schema.name;
    }

    public HsqlName getCatalogName() {
        return columnName.schema == null ? null
                                         : columnName.schema.schema;
    }

    @Override
    public String getCatalogNameString() {

        return columnName.schema == null ? null
                                         : columnName.schema.schema == null
                                           ? null
                                           : columnName.schema.schema.name;
    }

    public Grantee getOwner() {
        return columnName.schema == null ? null
                                         : columnName.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        switch (parameterMode) {

            case SchemaObject.ParameterModes.PARAM_IN :
                sb.append(Tokens.T_IN).append(' ');
                break;

            case SchemaObject.ParameterModes.PARAM_OUT :
                sb.append(Tokens.T_OUT).append(' ');
                break;

            case SchemaObject.ParameterModes.PARAM_INOUT :
                sb.append(Tokens.T_INOUT).append(' ');
                break;
        }

        if (columnName != null) {
            sb.append(columnName.statementName);
            sb.append(' ');
        }

        sb.append(dataType.getTypeDefinition());

        return sb.toString();
    }

    @Override
    public void setType(Type type) {
        this.dataType = type;
    }

    public void setName(HsqlName name) {
        this.columnName = name;
    }

    void setIdentity(NumberSequence sequence) {
        this.sequence = sequence;
        isIdentity    = sequence != null;
    }

    void setType(ColumnSchema other) {
        nullability = other.nullability;
        dataType    = other.dataType;
    }

    public NumberSequence getIdentitySequence() {
        return sequence;
    }

    /**
     *  Is column nullable.
     *
     * @return boolean
     */
    @Override
    public boolean isNullable() {

        boolean isNullable = super.isNullable();

        if (isNullable) {
            if (dataType.isDomainType()) {
                return dataType.userTypeModifier.isNullable();
            }
        }

        return isNullable;
    }

    @Override
    public byte getNullability() {
        return isPrimaryKey ? SchemaObject.Nullability.NO_NULLS
                            : super.getNullability();
    }

    public boolean isGenerated() {
        return generatingExpression != null;
    }

    public boolean hasDefault() {
        return getDefaultExpression() != null;
    }

    /**
     * Is column writeable or always generated
     *
     * @return boolean
     */
    @Override
    public boolean isWriteable() {
        return !isGenerated();
    }

    @Override
    public void setWriteable(boolean value) {
        throw Error.runtimeError(ErrorCode.U_S0500, "");
    }

    @Override
    public boolean isSearchable() {
        return Types.isSearchable(dataType.typeCode);
    }

    /**
     *  Is this single column primary key of the table.
     *
     * @return boolean
     */
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    /**
     *  Set primary key.
     *
     */
    void setPrimaryKey(boolean value) {
        isPrimaryKey = value;
    }

    /**
     *  Returns default value in the session context.
     */
    Object getDefaultValue(Session session) {

        return defaultExpression == null ? null
                                         : defaultExpression.getValue(session,
                                         dataType);
    }

    /**
     *  Returns generated value in the session context.
     */
    Object getGeneratedValue(Session session) {

        return generatingExpression == null ? null
                                            : generatingExpression.getValue(
                                            session, dataType);
    }

    /**
     *  Returns SQL for default value.
     */
    public String getDefaultSQL() {

        String ddl = null;

        ddl = defaultExpression == null ? null
                                        : defaultExpression.getSQL();

        return ddl;
    }

    /**
     *  Returns default expression for the column.
     */
    Expression getDefaultExpression() {

        if (defaultExpression == null) {
            if (dataType.isDomainType()) {
                return dataType.userTypeModifier.getDefaultClause();
            }

            return null;
        } else {
            return defaultExpression;
        }
    }

    void setDefaultExpression(Expression expr) {
        defaultExpression = expr;
    }

    /**
     *  Returns generated expression for the column.
     */
    Expression getGeneratingExpression() {
        return generatingExpression;
    }

    void setGeneratingExpression(Expression expr) {
        generatingExpression = expr;
    }

    public ColumnSchema duplicate() {

        ColumnSchema copy = new ColumnSchema(columnName, dataType,
                                             isNullable(), isPrimaryKey,
                                             defaultExpression);

        copy.setGeneratingExpression(generatingExpression);
        copy.setIdentity(sequence);

        return copy;
    }

    /*************** VOLTDB *********************/

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @param indent A string of whitespace to be prepended to every line
     * in the resulting XML.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    String voltGetXML(Session session, String indent) throws HSQLParseException
    {
        StringBuilder sb = new StringBuilder();

        // begin open column tag
        sb.append(indent).append("<column");
        // output column metadata
        sb.append(" name='").append(columnName.name).append("'");
        String typestring = Types.getTypeName(dataType.typeCode);
        sb.append(" type='").append(typestring).append("'");
        sb.append(" nullable='").append(isNullable()).append("'");
        sb.append(" size='").append(dataType.precision).append("'");

        if ((typestring.compareTo("VARCHAR") == 0) &&
            (dataType.precision > 1048576)) {
            String msg = "VARCHAR column size for column ";
            msg += getTableNameString() + "." + columnName.name;
            msg += " is > 1048576 char maximum.";
            throw new HSQLParseException(msg);
        }

        // see if there is a default value for the column
        Expression exp = getDefaultExpression();

        // if there is a default value for the column
        if (exp != null) {
            exp.dataType = dataType;

            // end open tag
            sb.append(">\n");
            // add default value to body of column element
            sb.append(indent + "  ").append("<default>\n");
            sb.append(exp.voltGetXML(session, indent + "    ")).append("\n");
            sb.append(indent + "  ").append("</default>\n");
            sb.append(indent + "</column>\n");
        }
        // if there is not a default value for the column
        else {
            // end self-closing open tag
            sb.append(" />\n");
        }

        return sb.toString();
    }
}
