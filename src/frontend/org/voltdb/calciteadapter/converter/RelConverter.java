/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.calciteadapter.converter;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Pair;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.SortDirectionType;

public class RelConverter {

    public static List<Pair<Integer, SortDirectionType>> convertCollation(RelCollation collation) {
        List<Pair<Integer, SortDirectionType>> collFields = new ArrayList<>();
        for(RelFieldCollation collField : collation.getFieldCollations()) {
            Direction dir = collField.getDirection();
            SortDirectionType voltDir = ("ASC".equalsIgnoreCase(dir.shortString)) ? SortDirectionType.ASC :
                SortDirectionType.DESC;
            int fieldIndex = collField.getFieldIndex();
            collFields.add(new Pair<>(fieldIndex, voltDir));
        }
        return collFields;
    }

    public static AbstractExpression convertDataTypeField(RelDataTypeField dataTypeField) {
        int columnIndex = dataTypeField.getIndex();
        int tableIndex = 0;
        String tableName = "";
        String columnName = String.format("%03d", columnIndex);

        TupleValueExpression tve = new TupleValueExpression(tableName, tableName, columnName, columnName, columnIndex, columnIndex);
        tve.setTableIndex(tableIndex);
        TypeConverter.setType(tve, dataTypeField.getType());
        return tve;
    }
}
