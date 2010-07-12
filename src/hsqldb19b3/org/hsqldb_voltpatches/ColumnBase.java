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

import org.hsqldb_voltpatches.types.Type;

/**
 * Base implementation of variables, columns of result or table.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ColumnBase {

    private String    name;
    private String    table;
    private String    schema;
    private String    catalog;
    private boolean   isWriteable;
    private boolean   isSearchable;
    protected byte     parameterMode;
    protected boolean isIdentity;
    protected byte     nullability;
    protected Type    dataType;

    ColumnBase() {}

    public ColumnBase(String catalog, String schema, String table,
                      String name) {

        this.catalog = catalog;
        this.schema  = schema;
        this.table   = table;
        this.name    = name;
    }

    public String getNameString() {
        return name;
    }

    public String getTableNameString() {
        return table;
    }

    public String getSchemaNameString() {
        return schema;
    }

    public String getCatalogNameString() {
        return catalog;
    }

    public void setIdentity(boolean value) {
        isIdentity = value;
    }

    public boolean isIdentity() {
        return isIdentity;
    }

    protected void setType(ColumnBase other) {
        nullability = other.nullability;
        dataType    = other.dataType;
    }

    public void setType(Type type) {
        this.dataType = type;
    }

    public boolean isNullable() {
        return !isIdentity && nullability == SchemaObject.Nullability.NULLABLE;
    }

    protected void setNullable(boolean value) {
        nullability = value ? SchemaObject.Nullability.NULLABLE
                            : SchemaObject.Nullability.NO_NULLS;
    }

    public byte getNullability() {
        return isIdentity ? SchemaObject.Nullability.NO_NULLS
                          : nullability;
    }

    public void setNullability(byte value) {
        nullability = value;
    }

    public boolean isWriteable() {
        return isWriteable;
    }

    public boolean isSearchable() {
        return isSearchable;
    }

    public void setWriteable(boolean value) {
        isWriteable = value;
    }

    public Type getDataType() {
        return dataType;
    }

    public byte getParameterMode() {
        return parameterMode;
    }

    public void setParameterMode(byte mode) {
        this.parameterMode = mode;
    }
}
