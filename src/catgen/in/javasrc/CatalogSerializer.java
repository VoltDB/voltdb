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

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CatalogSerializer implements CatalogVisitor {

    private final StringBuilder m_builder;
    private Set<String> m_fieldFilter;
    private Set<Class<? extends CatalogType>> m_childFilter;


    public CatalogSerializer() {
        m_builder = new StringBuilder();
    }

    public CatalogSerializer withChildFilter(Set<Class<? extends CatalogType>> childFilter) {
        m_childFilter = childFilter;
        return this;
    }

    public CatalogSerializer withFieldFilter(Set<String> fieldFilter) {
        m_fieldFilter = fieldFilter;
        return this;
    }

    @Override
    public void visit(CatalogType ct) {
        writeCreationCommand(ct);
        writeFieldCommands(ct);
        writeChildCommands(ct);
    }

    public String getResult() {
        return m_builder.toString();
    }

    void writeCreationCommand(CatalogType ct) {
        // Catalog does not need a creation command.
        if (ct instanceof Catalog) {
            return;
        }
        m_builder.append("add ");
        ct.m_parentMap.m_parent.getCatalogPath(m_builder);
        m_builder.append(' ');
        m_builder.append(ct.m_parentMap.m_name);
        m_builder.append(' ');
        m_builder.append(ct.m_typename);
        m_builder.append("\n");
    }

    void writeFieldCommands(CatalogType ct) {
        int i = 0;
        for (String field : ct.getFields()) {
            if (m_fieldFilter == null || m_fieldFilter.contains(field)) {
                writeCommandForField(ct, field, i == 0);
                ++i;
            }
        }
        m_fieldFilter = null;
    }

    void writeCommandForField(CatalogType ct, String field, boolean printFullPath) {
        m_builder.append("set ");
        if (printFullPath) {
            ct.getCatalogPath(m_builder);
            m_builder.append(' ');
        }
        else {
            m_builder.append("$PREV "); // use caching to shrink output + speed parsing
        }
        m_builder.append(field).append(' ');
        Object value = ct.getField(field);
        if (value == null) {
            m_builder.append("null");
        }
        else if (value.getClass() == Integer.class)
            m_builder.append(value);
        else if (value.getClass() == Boolean.class)
            m_builder.append(Boolean.toString((Boolean)value));
        else if (value.getClass() == String.class)
            m_builder.append("\"").append(value).append("\"");
        else if (value instanceof CatalogType)
            ((CatalogType)value).getCatalogPath(m_builder);
        else
            throw new CatalogException("Unsupported field type '" + value + "'");
        m_builder.append("\n");
    }

    void writeChildCommands(CatalogType ct) {
        String[] childCollections = ct.getChildCollections();
        List<CatalogMap<? extends CatalogType>> mapsToVisit =
                new ArrayList<>(childCollections.length);

        for (String childCollection : ct.getChildCollections()) {
            CatalogMap<? extends CatalogType> map = ct.getCollection(childCollection);
            if (m_childFilter == null || m_childFilter.contains(map.m_cls)) {
                mapsToVisit.add(map);
            }
        }
        m_childFilter = null;
        for (CatalogMap<? extends CatalogType> map : mapsToVisit) {
            map.accept(this);
        }
    }

    void writeDeleteDiffStatement(CatalogType ct, String parentName) {
        m_builder.append("delete ").append(ct.getParent().getCatalogPath()).append(" ")
                 .append(parentName).append(" ").append(ct.getTypeName()).append("\n");
    }

    public static String getDeleteDiffStatement(CatalogType ct, String parentName) {
        CatalogSerializer serializer = new CatalogSerializer();
        serializer.writeDeleteDiffStatement(ct, parentName);
        return serializer.getResult();
    }
}
