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

import java.lang.reflect.Field;


/**
 * The base class for all objects in the Catalog. CatalogType instances all
 * have a name and a path (from the root). They have fields and children.
 * All fields are simple types. All children are CatalogType instances.
 *
 * MUST BE THREAD SAFE, MUITIPLE THREADS ACCESS IT!
 */
public abstract class CatalogType implements Comparable<CatalogType> {

    private static final char MAP_SEPARATOR = '#';

    class CatalogReference<T extends CatalogType> {

        volatile T m_value = null;
        volatile String m_unresolvedPath = null;
        Object m_lock = new Object();

        public void setUnresolved(String path) {
            synchronized (m_lock) {
                // if null: value will be set to null
                m_value = null;
                m_unresolvedPath = path;
            }
        }

        public void set(T value) {
            synchronized (m_lock) {
                m_value = value;
                m_unresolvedPath = null;
            }
        }

        @SuppressWarnings("unchecked")
        T resolve() {
            synchronized (m_lock) {
                if (m_unresolvedPath != null) {
                    m_value = (T) getCatalog().getCatalogOperator().getItemForPath(m_unresolvedPath);
                    m_unresolvedPath = null;
                }
                return m_value;
            }
        }

        public T get() {
            return m_unresolvedPath == null ? m_value : resolve();
        }

        public String getPath() {
            if (m_unresolvedPath != null) return m_unresolvedPath;
            else if (m_value != null) return m_value.getCatalogPath();
            else return null;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            // TODO Auto-generated method stub
            return super.hashCode();
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CatalogReference<?>) {
                String myPath = getPath();
                String otherPath = ((CatalogReference<?>) obj).getPath();
                if (myPath == null) return otherPath == null;
                return myPath.equals(otherPath);
            }
            return false;
        }
    }

    String m_typename;
    CatalogMap<? extends CatalogType> m_parentMap;
    Integer m_relativeIndex = null;

    // Annotation where additional non-runtime info can be squirreled away
    // Used by the compiler report generator for now
    Object m_annotation = null;

    //Attachment to store expensive to materialize state
    Object m_attachment = null;

    /**
     * Accept a {@link CatalogVisitor}.
     * @param visitor the visitor.
     */
    public void accept(CatalogVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Gets any annotation added to this instance.
     * @return Annotation object or null.
     */
    public Object getAnnotation() {
        return m_annotation;
    }

    /**
     * Sets the annotation object for this instance.
     * @param annotation Annotation object or null.
     */
    public void setAnnotation(Object annotation) {
        m_annotation = annotation;
    }

    public Object getAttachment() {
        return m_attachment;
    }

    public void setAttachment(Object attachment) {
        m_attachment = attachment;
    }

    /**
     * Get the full catalog path of this CatalogType instance
     * @return The full catalog path of this CatalogType instance
     */
    String getCatalogPath() {
        return m_parentMap.getPath() + MAP_SEPARATOR + m_typename;
    }

    String getParentMapName() {
        return m_parentMap.m_name;
    }

    /**
     * Get the name of this CatalogType instance
     * @return The name of this CatalogType instance
     */
    public String getTypeName() {
        return m_typename;
    }

    /**
     * Get the parent of this CatalogType instance
     * @return The parent of this CatalogType instance
     */
    public CatalogType getParent() {
        // parent map only null for class Catalog which overrides this
        assert(m_parentMap != null);
        return m_parentMap.m_parent;
    }

    /**
    * Get the root catalog object for this item
    * @return The base Catalog object
    */
    public Catalog getCatalog() {
        return m_parentMap.m_catalog;
    }

    /**
     * Get the index of this catalog node relative to its
     * siblings
     * @return The index of this CatalogType instance
     */
    public int getRelativeIndex() {
        if (m_relativeIndex == null) {
            m_parentMap.recomputeRelativeIndexes();
        }
        return m_relativeIndex;
    }

    /**
     * Get the set of field names of the fields of this CatalogType
     * @return The set of field names
     */
    public abstract String[] getFields();

    abstract String[] getChildCollections();

    /**
     * Get the value of a field knowing only the name of the field
     * @param field The name of the field being requested
     * @return The field requested or null
     */
    public abstract Object getField(String field);

    /**
     * This is my lazy hack to avoid using reflection to instantiate records.
     */
    void setBaseValues(CatalogMap<? extends CatalogType> parentMap, String name) {
        if (name == null) {
            throw new CatalogException("Null value where it shouldn't be.");
        }
        m_parentMap = parentMap;
        m_typename = name;
    }

    abstract void initChildMaps();

    @SuppressWarnings("unchecked")
    CatalogMap<? extends CatalogType> getCollection(String collectionName) {
        try {
            return (CatalogMap<? extends CatalogType>) getField(collectionName);
        }
        catch (ClassCastException | NullPointerException e) {
            throw new CatalogException("Collection name given isn't a collection.");
        }
    }

    abstract void set(String field, String value);

    @Override
    public int compareTo(CatalogType o) {
        if (this == o) {
            return 0;
        }
        // null comparands will throw an exception here:
        return getCatalogPath().compareTo(o.getCatalogPath());
    }

    abstract public void copyFields(CatalogType obj);

    CatalogType deepCopy(Catalog catalog, CatalogMap<? extends CatalogType> parentMap) {

        CatalogType copy = null;
        try {
            copy = getClass().newInstance();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        assert(parentMap.m_parent.getCatalog() == catalog);
        copy.setBaseValues(parentMap, m_typename);
        copy.initChildMaps();
        copy.m_relativeIndex = m_relativeIndex;

        copyFields(copy);

        return copy;
    }

    /**
     * Produce a more readable string representation that a simple
     * hash code.
     */
    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "{" + m_typename + "}");
    }

    @Override
    public int hashCode()
    {
        int result = 1;
        // Generate something reasonably unique but consistent for this element
        result = 37 * result + getCatalogPath().hashCode();
        result = 31 * result + m_typename.hashCode();
        return result;
    }

    /**
     * Fails an assertion if any child of this object doesn't think
     * it's part of the same catalog.
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public void validate() throws IllegalArgumentException, IllegalAccessException {
        for (Field field : getClass().getDeclaredFields()) {
            if (CatalogType.class.isAssignableFrom(field.getType())) {
                CatalogType ct = (CatalogType) field.get(this);
                assert(ct.getCatalog() == getCatalog()) : ct.getCatalogPath() + " has wrong catalog";
            }
            if (CatalogReference.class.isAssignableFrom(field.getType())) {
                @SuppressWarnings("unchecked")
                CatalogReference<? extends CatalogType> cr = (CatalogReference<? extends CatalogType>) field.get(this);
                if (cr.m_value != null) {
                    assert(cr.m_value.getCatalog() == getCatalog()) : cr.m_value.getCatalogPath() + " has wrong catalog";
                }
            }
            if (CatalogMap.class.isAssignableFrom(field.getClass())) {
                @SuppressWarnings("unchecked")
                CatalogMap<? extends CatalogType> cm = (CatalogMap<? extends CatalogType>) field.get(this);
                for (CatalogType ct : cm) {
                    assert(ct.getCatalog() == getCatalog()) : ct.getCatalogPath() + " has wrong catalog";
                    ct.validate();
                }
            }
        }
    }
}
