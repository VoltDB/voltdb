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

import com.google_voltpatches.common.cache.Cache;
import com.google_voltpatches.common.cache.CacheBuilder;

/**
 * The root class in the Catalog hierarchy, which is essentially a tree of
 * instances of CatalogType objects, accessed by paths globally, and child
 * names when given a parent.
 */
public final class Catalog extends CatalogType {

    public static final char MAP_SEPARATOR = '#';

    Cache<String, CatalogType> m_pathCache = CacheBuilder.newBuilder().maximumSize(8).build();

    CatalogMap<Cluster> m_clusters;

    private final CatalogOperator m_operator = new CatalogOperator(this);

    /**
     * Create a new Catalog hierarchy.
     */
    public Catalog() {
        setBaseValues(null, "catalog");
        m_clusters = new CatalogMap<Cluster>(this, this, "clusters", Cluster.class, 1);
        m_relativeIndex = 1;
    }

    @Override
    void initChildMaps() {
        // never called on the root catalog object
    }

    @Override
    public Catalog getCatalog() {
        return this;
    }

    /**
     * Get the operator that can make changes to this catalog object.
     * @return the catalog operator.
     */
    @Override
    public CatalogOperator getOperator() {
        return m_operator;
    }

    @Override
    public String getCatalogPath() {
        return "/";
    }

    @Override
    public void getCatalogPath(StringBuilder sb) {
        sb.append('/');
    }

    @Override
    public CatalogType getParent() {
        return null;
    }

    public Catalog deepCopy() {
        Catalog copy = new Catalog();
        // Note that CatalogType.deepCopy isn't called on the catalog node.
        // need to fully compensate for that here.
        copy.m_relativeIndex = 1;
        copy.m_clusters.copyFrom(m_clusters);

        return copy;
    }

    /** GETTER: The set of the clusters in this catalog */
    public CatalogMap<Cluster> getClusters() {
        return m_clusters;
    }

    @Override
    public String[] getFields() {
        return new String[] {};
    }

    @Override
    String[] getChildCollections() {
        return new String[] { "clusters" };
    }

    @Override
    public Object getField(String field) {
        switch (field) {
        case "clusters":
            return getClusters();
        default:
            throw new CatalogException(String.format("Unknown field: %s in class %s",
                    field, getClass().getSimpleName()));
        }
    }

    @Override
    void set(String field, String value) {
        throw new CatalogException("No fields to set in Catalog base object.");
    }

    @Override
    void copyFields(CatalogType obj) {
        // no fields to copy
        // also not used as Catalog overrides the calling method of CatalogType
    }

    @Override
    public boolean equals(Object obj) {
        // this isn't really the convention for null handling
        if ((obj == null) || (obj.getClass().equals(getClass()) == false))
            return false;

        // Do the identity check
        if (obj == this)
            return true;

        // this is safe because of the class check
        // it is also known that the childCollections var will be the same
        //  from the class check
        Catalog other = (Catalog) obj;

        // are the fields / children the same? (deep compare)
        if ((m_clusters == null) != (other.m_clusters == null)) return false;
        if ((m_clusters != null) && !m_clusters.equals(other.m_clusters)) return false;

        return true;
    }
}
