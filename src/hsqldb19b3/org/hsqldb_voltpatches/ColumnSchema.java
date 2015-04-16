/* Copyright (c) 2001-2011, The HSQL Development Group
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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.RangeGroup.RangeGroupSimple;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/**
 * Implementation of SQL table column metadata.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.9
 * @since 1.9.0
 */
public final class ColumnSchema extends ColumnBase implements SchemaObject {

    public static final ColumnSchema[] emptyArray = new ColumnSchema[]{};

    //
    private HsqlName       columnName;
    private boolean        isPrimaryKey;
    private Expression     defaultExpression;
    private Expression     generatingExpression;
    private NumberSequence sequence;
    private OrderedHashSet references;
    private OrderedHashSet generatedColumnReferences;
    private Expression     accessor;

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

        setReferences();
    }

    public int getType() {
        return columnName.type;
    }

    public HsqlName getName() {
        return columnName;
    }

    public String getNameString() {
        return columnName.name;
    }

    public String getTableNameString() {
        return columnName.parent == null ? null
                                         : columnName.parent.name;
    }

    public HsqlName getSchemaName() {
        return columnName.schema;
    }

    public String getSchemaNameString() {
        return columnName.schema == null ? null
                                         : columnName.schema.name;
    }

    public HsqlName getCatalogName() {
        return columnName.schema == null ? null
                                         : columnName.schema.schema;
    }

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
        return references;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject table) {

        if (generatingExpression == null) {
            return;
        }

        generatingExpression.resetColumnReferences();
        generatingExpression.resolveCheckOrGenExpression(
            session,
            new RangeGroupSimple(((Table) table).getDefaultRanges(), false),
            false);

        if (dataType.typeComparisonGroup
                != generatingExpression.getDataType().typeComparisonGroup) {
            throw Error.error(ErrorCode.X_42561);
        }

        setReferences();
    }

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

    public long getChangeTimestamp() {
        return 0;
    }

    public void setType(Type type) {

        this.dataType = type;

        setReferences();
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
    public boolean isNullable() {

        boolean isNullable = super.isNullable();

        if (isNullable) {
            if (dataType.isDomainType()) {
                return dataType.userTypeModifier.isNullable();
            }
        }

        return isNullable;
    }

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
    public boolean isWriteable() {
        return !isGenerated();
    }

    public void setWriteable(boolean value) {
        throw Error.runtimeError(ErrorCode.U_S0500, "ColumnSchema");
    }

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
    public Object getDefaultValue(Session session) {

        return defaultExpression == null ? null
                                         : defaultExpression.getValue(session,
                                         dataType);
    }

    /**
     *  Returns generated value in the session context.
     */
    public Object getGeneratedValue(Session session) {

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
    public Expression getGeneratingExpression() {
        return generatingExpression;
    }

    void setGeneratingExpression(Expression expr) {
        generatingExpression = expr;
    }

    public ColumnSchema duplicate() {

        ColumnSchema copy = new ColumnSchema(columnName, dataType, true,
                                             isPrimaryKey, defaultExpression);

        copy.setNullability(this.nullability);
        copy.setGeneratingExpression(generatingExpression);
        copy.setIdentity(sequence);

        return copy;
    }

    public Expression getAccessor() {

        if (accessor == null) {
            accessor = new ExpressionColumnAccessor(this);
        }

        return accessor;
    }

    public OrderedHashSet getGeneratedColumnReferences() {
        return generatedColumnReferences;
    }

    private void setReferences() {

        if (references != null) {
            references.clear();
        }

        if (generatedColumnReferences != null) {
            generatedColumnReferences.clear();
        }

        if (dataType.isDomainType() || dataType.isDistinctType()) {
            HsqlName name = ((SchemaObject) dataType).getName();

            if (references == null) {
                references = new OrderedHashSet();
            }

            references.add(name);
        }

        if (generatingExpression != null) {
            OrderedHashSet set = new OrderedHashSet();

            generatingExpression.collectObjectNames(set);

            Iterator it = set.iterator();

            while (it.hasNext()) {
                HsqlName name = (HsqlName) it.next();

                if (name.type == SchemaObject.COLUMN
                        || name.type == SchemaObject.TABLE) {
                    if (name.type == SchemaObject.COLUMN) {
                        if (generatedColumnReferences == null) {
                            generatedColumnReferences = new OrderedHashSet();
                        }

                        generatedColumnReferences.add(name);
                    }
                } else {
                    if (references == null) {
                        references = new OrderedHashSet();
                    }

                    references.add(name);
                }
            }
        }
    }
    /************************* Volt DB Extensions *************************/

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
     */
    VoltXMLElement voltGetColumnXML(Session session)
            throws org.hsqldb_voltpatches.HSQLInterface.HSQLParseException
    {
        VoltXMLElement column = new VoltXMLElement("column");

        // output column metadata
        column.attributes.put("name", columnName.name);
        // TODO: consider breaking sqlTypeToString out from SqlFIle
        // to somewhere more convenient like Types.java.
        String typestring = dataType.getNameString();
        column.attributes.put("valuetype", typestring);
        column.attributes.put("nullable", String.valueOf(isNullable()));
        column.attributes.put("size", String.valueOf(dataType.precision));

        if (dataType.precision > 1048576) {
            String msg = typestring + " column size for column ";
            msg += getTableNameString() + "." + columnName.name;
            msg += " is > 1048576 char maximum.";
            throw new org.hsqldb_voltpatches.HSQLInterface.HSQLParseException(msg);
        }

        if (typestring.compareTo("VARCHAR") == 0) {
            assert(dataType instanceof org.hsqldb_voltpatches.types.CharacterType);
            boolean inBytes = ((org.hsqldb_voltpatches.types.CharacterType)dataType).inBytes;
            column.attributes.put("bytes", String.valueOf(inBytes));
        }

        // see if there is a default value for the column
        Expression exp = getDefaultExpression();

        // if there is a default value for the column
        // and the column value is not NULL. (Ignore "DEFAULT NULL" in DDL)
        if (exp != null) {
            if (exp.valueData != null || (exp instanceof FunctionSQL && ((FunctionSQL)exp).isValueFunction())) {
                exp.dataType = dataType;

                // add default value to body of column element
                VoltXMLElement defaultElem = new VoltXMLElement("default");
                column.children.add(defaultElem);
                defaultElem.children.add(exp.voltGetXML(session));
            }
        }

        return column;
    }
    /**********************************************************************/
}
