/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Helps serialize the VoltDB catalog.
 * @author Yiqun Zhang
 * @since 9.0
 */
public final class CatalogSerializer implements CatalogVisitor {

    private final StringBuilder m_builder;
    private final Set<String> m_fieldFilter;
    private final Set<Class<? extends CatalogType>> m_childFilter;

    /**
     * Create a default catalog serializer.
     */
    CatalogSerializer() {
        this(null, null);
    }

    /**
     * Create a catalog serializer with a field filter and a child filter.
     * @param fieldFilter only fields included in this set will be serialized.
     * @param childFilter only children included in this set will be serialized.
     */
    CatalogSerializer(Set<String> fieldFilter,
            Set<Class<? extends CatalogType>> childFilter) {
        m_builder = new StringBuilder();
        m_fieldFilter = fieldFilter;
        m_childFilter = childFilter;
    }

    @Override
    public void visit(CatalogType ct) {
        writeCreationCommand(ct);
        writeFieldCommands(ct);
        writeChildCommands(ct);
    }

    /**
     * Return the serialized catalog.
     * @return
     */
    String getResult() {
        return m_builder.toString();
    }

    private void writeFieldCommands(CatalogType ct) {
        int i = 0;
        for (String field : ct.getFields()) {
            if (m_fieldFilter == null || m_fieldFilter.contains(field)) {
                writeCommandForField(ct, field, i == 0);
                ++i;
            }
        }
    }

    private native void writeCreationCommand(CatalogType ct);
    native void writeDeleteDiffStatement(CatalogType ct, String parentName);

    void writeCommandForField(CatalogType ct, String field, boolean printFullPath) {
        m_builder.append("set ");
        if (printFullPath) {
            m_builder.append(ct.getCatalogPath()).append(' ');
        } else {
            m_builder.append("$PREV "); // use caching to shrink output + speed parsing
        }
        m_builder.append(field).append(' ');
        Object value = ct.getField(field);
        if (value == null) {
            m_builder.append("null");
        } else if (value.getClass() == Byte.class || value.getClass() == Integer.class || value.getClass() == Boolean.class) {
                m_builder.append(value);
        } else if (value.getClass() == String.class) {
            m_builder.append("\"").append(value).append("\"");
        } else if (value instanceof CatalogType) {
            m_builder.append(((CatalogType)value).getCatalogPath());
        } else {
            throw new CatalogException("Unsupported field value '" +   value + "' type '" + value.getClass() + "'");
        }
        m_builder.append("\n");
    }

    private void writeChildCommands(CatalogType ct) {
        String[] childCollections = ct.getChildCollections();
        List<CatalogMap<? extends CatalogType>> mapsToVisit =
                new ArrayList<>(childCollections.length);

        for (String childCollection : ct.getChildCollections()) {
            CatalogMap<? extends CatalogType> map = ct.getCollection(childCollection);
            if (m_childFilter == null || m_childFilter.contains(map.m_cls)) {
                mapsToVisit.add(map);
            }
        }
        for (CatalogMap<? extends CatalogType> map : mapsToVisit) {
            map.accept(this);
        }
    }

    public static String getDeleteDiffStatement(CatalogType ct, String parentName) {
        CatalogSerializer serializer = new CatalogSerializer();
        serializer.writeDeleteDiffStatement(ct, parentName);
        return serializer.getResult();
    }
}
