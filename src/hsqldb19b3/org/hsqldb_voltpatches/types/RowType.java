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


package org.hsqldb_voltpatches.types;

import org.hsqldb_voltpatches.OpTypes;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.SortAndSlice;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;

/**
 * Class for ROW type objects.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.9
 * @since 2.0.0
 */
public class RowType extends Type {

    final Type[]    dataTypes;
    TypedComparator comparator;

    public RowType(Type[] dataTypes) {

        super(Types.SQL_ROW, Types.SQL_ROW, 0, 0);

        this.dataTypes = dataTypes;
    }

    public int displaySize() {
        return 0;
    }

    public int getJDBCTypeCode() {
        return Types.NULL;
    }

    public Class getJDBCClass() {
        return java.sql.ResultSet.class;
    }

    public String getJDBCClassName() {
        return "java.sql.ResultSet";
    }

    public int getJDBCScale() {
        return 0;
    }

    public int getJDBCPrecision() {
        return 0;
    }

    public int getSQLGenericTypeCode() {
        return Types.SQL_ROW;
    }

    public boolean isRowType() {
        return true;
    }

    public String getNameString() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_ROW);
        sb.append('(');

        for (int i = 0; i < dataTypes.length; i++) {
            if (i > 0) {
                sb.append(',');
            }

            sb.append(dataTypes[i].getDefinition());
        }

        sb.append(')');

        return sb.toString();
    }

    public String getDefinition() {
        return getNameString();
    }

    public int compare(Session session, Object a, Object b) {

        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        Object[] arra   = (Object[]) a;
        Object[] arrb   = (Object[]) b;
        int      length = arra.length;

        if (arrb.length < length) {
            length = arrb.length;
        }

        for (int i = 0; i < length; i++) {
            int result = dataTypes[i].compare(session, arra[i], arrb[i]);

            if (result != 0) {
                return result;
            }
        }

        if (arra.length > arrb.length) {
            return 1;
        } else if (arra.length < arrb.length) {
            return -1;
        }

        return 0;
    }

    public Object convertToTypeLimits(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        Object[] arra = (Object[]) a;
        Object[] arrb = new Object[arra.length];

        for (int i = 0; i < arra.length; i++) {
            arrb[i] = dataTypes[i].convertToTypeLimits(session, arra[i]);
        }

        return arrb;
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {

        if (a == null) {
            return null;
        }

        if (otherType == null) {
            return a;
        }

        if (!otherType.isRowType()) {
            throw Error.error(ErrorCode.X_42562);
        }

        Type[] otherTypes = ((RowType) otherType).getTypesArray();

        if (dataTypes.length != otherTypes.length) {
            throw Error.error(ErrorCode.X_42564);
        }

        Object[] arra = (Object[]) a;
        Object[] arrb = new Object[arra.length];

        for (int i = 0; i < arra.length; i++) {
            arrb[i] = dataTypes[i].convertToType(session, arra[i],
                                                 otherTypes[i]);
        }

        return arrb;
    }

    public Object convertToDefaultType(SessionInterface sessionInterface,
                                       Object o) {
        return o;
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return convertToSQLString(a);
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return Tokens.T_NULL;
        }

        Object[]     array = (Object[]) a;
        StringBuffer sb    = new StringBuffer();

        sb.append(Tokens.T_ROW);
        sb.append('(');

        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                sb.append(',');
            }

            String string = dataTypes[i].convertToSQLString(array[i]);

            sb.append(string);
        }

        sb.append(')');

        return sb.toString();
    }

    public boolean canConvertFrom(Type otherType) {

        if (otherType == null) {
            return true;
        }

        if (!otherType.isRowType()) {
            return false;
        }

        Type[] otherTypes = ((RowType) otherType).getTypesArray();

        if (dataTypes.length != otherTypes.length) {
            return false;
        }

        for (int i = 0; i < dataTypes.length; i++) {
            if (!dataTypes[i].canConvertFrom(otherTypes[i])) {
                return false;
            }
        }

        return true;
    }

    public boolean canBeAssignedFrom(Type otherType) {

        if (otherType == null) {
            return true;
        }

        if (!otherType.isRowType()) {
            return false;
        }

        Type[] otherTypes = ((RowType) otherType).getTypesArray();

        if (dataTypes.length != otherTypes.length) {
            return false;
        }

        for (int i = 0; i < dataTypes.length; i++) {
            if (!dataTypes[i].canBeAssignedFrom(otherTypes[i])) {
                return false;
            }
        }

        return true;
    }

    public Type getAggregateType(Type other) {

        if (other == null) {
            return this;
        }

        if (other == SQL_ALL_TYPES) {
            return this;
        }

        if (other == this) {
            return this;
        }

        if (!other.isRowType()) {
            throw Error.error(ErrorCode.X_42562);
        }

        Type[] newTypes   = new Type[dataTypes.length];
        Type[] otherTypes = ((RowType) other).getTypesArray();

        if (dataTypes.length != otherTypes.length) {
            throw Error.error(ErrorCode.X_42564);
        }

        for (int i = 0; i < dataTypes.length; i++) {
            newTypes[i] = dataTypes[i].getAggregateType(otherTypes[i]);
        }

        return new RowType(newTypes);
    }

    public Type getCombinedType(Session session, Type other, int operation) {

        if (operation != OpTypes.CONCAT) {
            return getAggregateType(other);
        }

        if (other == null) {
            return this;
        }

        if (!other.isRowType()) {
            throw Error.error(ErrorCode.X_42562);
        }

        Type[] newTypes   = new Type[dataTypes.length];
        Type[] otherTypes = ((RowType) other).getTypesArray();

        if (dataTypes.length != otherTypes.length) {
            throw Error.error(ErrorCode.X_42564);
        }

        for (int i = 0; i < dataTypes.length; i++) {
            newTypes[i] = dataTypes[i].getAggregateType(otherTypes[i]);
        }

        return new RowType(newTypes);
    }

    public Type[] getTypesArray() {
        return dataTypes;
    }

    public int compare(Session session, Object a, Object b,
                       SortAndSlice sort) {

        if (a == b) {
            return 0;
        }

        // not related to sort
        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        Object[] arra   = (Object[]) a;
        Object[] arrb   = (Object[]) b;
        int      length = sort.sortOrder.length;

        for (int i = 0; i < length; i++) {
            a = arra[sort.sortOrder[i]];
            b = arrb[sort.sortOrder[i]];

            if (a == b) {
                continue;
            }

            if (sort.sortNullsLast[i]) {
                if (a == null) {
                    return 1;
                }

                if (b == null) {
                    return -1;
                }
            }

            int result = dataTypes[i].compare(session, a, b);

            if (result != 0) {
                if (sort.sortDescending[i]) {
                    return -result;
                }

                return result;
            }
        }

        return 0;
    }

    public boolean equals(Object other) {

        if (other == this) {
            return true;
        }

        if (other instanceof Type) {
            if (((Type) other).typeCode != Types.SQL_ROW) {
                return false;
            }

            Type[] otherTypes = ((RowType) other).dataTypes;

            if (otherTypes.length != dataTypes.length) {
                return false;
            }

            for (int i = 0; i < dataTypes.length; i++) {
                if (!dataTypes[i].equals(otherTypes[i])) {
                    return false;
                }

                return true;
            }
        }

        return false;
    }

    public int hashCode(Object a) {

        if (a == null) {
            return 0;
        }

        int      hash  = 0;
        Object[] array = (Object[]) a;

        for (int i = 0; i < dataTypes.length && i < 4; i++) {
            hash += dataTypes[i].hashCode(array[i]);
        }

        return hash;
    }

    synchronized TypedComparator getComparator(Session session) {

        if (comparator == null) {
            TypedComparator c    = Type.newComparator(session);
            SortAndSlice    sort = new SortAndSlice();

            sort.prepareMultiColumn(dataTypes.length);
            c.setType(this, sort);

            comparator = c;
        }

        return comparator;
    }

    public static String convertToSQLString(Object[] array, Type[] types,
            int maxUnitLength) {

        if (array == null) {
            return Tokens.T_NULL;
        }

        StringBuffer sb = new StringBuffer();

        sb.append('(');

        for (int i = 0; i < array.length; i++) {
            String value;

            if (i > 0) {
                sb.append(',');
            }

            String string = types[i].convertToSQLString(array[i]);

            if (maxUnitLength > 10 && string.length() > maxUnitLength) {
                sb.append(string.substring(0, maxUnitLength - 4));
                sb.append(" ...");
            } else {
                sb.append(string);
            }
        }

        sb.append(')');

        return sb.toString();
    }
}
