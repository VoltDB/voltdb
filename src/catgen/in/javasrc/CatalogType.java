/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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


/**
 * The base class for all objects in the Catalog. CatalogType instances all
 * have a name and a path (from the root). They have fields and children.
 * All fields are simple types. All children are CatalogType instances.
 *
 */
public abstract class CatalogType implements Comparable<CatalogType> {

    class CatalogReference<T extends CatalogType> {

        T m_value = null;
        String m_unresolvedPath = null;

        public void setUnresolved(String path) {
            // if null: value will be set to null
            m_value = null;
            m_unresolvedPath = path;
        }

        public void set(T value) {
            m_value = value;
            m_unresolvedPath = null;
        }

        @SuppressWarnings("unchecked")
        T resolve() {
            if (m_unresolvedPath != null) {
                m_value = (T) m_catalog.getItemForRef(m_unresolvedPath);
                m_unresolvedPath = null;
            }
            return m_value;
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
    Catalog m_catalog;
    int m_relativeIndex;

    // Annotation where additional non-runtime info can be squirreled away
    // Used by the compiler report generator for now
    Object m_annotation = null;

    //Attachment to store expensive to materialize state
    Object m_attachment = null;

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
        return m_parentMap.getPath() + "[" + m_typename + "]";
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
        return m_catalog;
    }

    /**
     * Get the index of this catalog node relative to its
     * siblings
     * @return The index of this CatalogType instance
     */
    public int getRelativeIndex() {
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
     * This should only ever be called from CatalogMap.add(); it's my lazy hack
     * to avoid using reflection to instantiate records.
     */
    void setBaseValues(Catalog catalog, CatalogMap<? extends CatalogType> parentMap, String name) {
        if ((name == null) || (catalog == null)) {
            throw new CatalogException("Null value where it shouldn't be.");
        }
        m_catalog = catalog;
        m_parentMap = parentMap;
        m_typename = name;
        catalog.registerGlobally(this);
    }

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

    void writeCreationCommand(StringBuilder sb) {
        String path = getCatalogPath();

        // skip root node command
        if (path.equals("/"))
            return;

        int lastSlash = path.lastIndexOf("/");
        String key = path.substring(lastSlash + 1);
        String newPath = path.substring(0, lastSlash);
        if (newPath.length() == 0)
            newPath = "/";
        String[] parts = key.split("\\[");
        if (parts.length < 2) {
            int x = 5;
        }
        parts[1] = parts[1].substring(0, parts[1].length() - 1);
        parts[1] = parts[1].trim();

        sb.append("add ").append(newPath).append(" ");
        sb.append(parts[0]).append(" ").append(parts[1]);
        sb.append("\n");
    }

    void writeCommandForField(StringBuilder sb, String field, boolean printFullPath) {
        String path = getCatalogPath();
        if (!printFullPath) path = "$PREV"; // use cacheing to shrink output + speed parsing

        sb.append("set ").append(path).append(" ");
        sb.append(field).append(" ");
        Object value = getField(field);
        if (value == null) {
            if ((field.equals("partitioncolumn")) && (getCatalogPath().equals("/clusters[cluster]/databases[database]/procedures[delivery]")))
                System.out.printf("null for field %s at path %s\n", field, getCatalogPath());
            sb.append("null");

        }
        else if (value.getClass() == Integer.class)
            sb.append(value);
        else if (value.getClass() == Boolean.class)
            sb.append(Boolean.toString((Boolean)value));
        else if (value.getClass() == String.class)
            sb.append("\"").append(value).append("\"");
        else if (value instanceof CatalogType)
            sb.append(((CatalogType)value).getCatalogPath());
        else
            throw new CatalogException("Unsupported field type '" + value + "'");
        sb.append("\n");
    }

    void writeFieldCommands(StringBuilder sb) {
        int i = 0;
        for (String field : getFields()) {
            writeCommandForField(sb, field, i == 0);
            ++i;
        }
    }

    void writeChildCommands(StringBuilder sb)  {
        for (String childCollection : getChildCollections()) {
            CatalogMap<? extends CatalogType> map = getCollection(childCollection);
            map.writeCommandsForMembers(sb);
        }
    }

    @Override
    public int compareTo(CatalogType o) {
        if (this == o) {
            return 0;
        }
        // null comparands will throw an exception here:
        return getCatalogPath().compareTo(o.getCatalogPath());
    }

    abstract void copyFields(CatalogType obj);

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
        copy.setBaseValues(catalog, parentMap, m_typename);
        copy.m_relativeIndex = m_relativeIndex;

        copyFields(copy);

        catalog.registerGlobally(copy);

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
     */
    public void validate() {
        /*for (Entry<String, Object> e : m_fields.entrySet()) {
            Object value = e.getValue();
            if (value instanceof CatalogType) {
                CatalogType ct = (CatalogType) value;
                assert(ct.getCatalog() == getCatalog()) : ct.getPath() + " has wrong catalog";
            }
        }

        for (Entry<String, CatalogMap<? extends CatalogType>> e : m_childCollections.entrySet()) {
            for (CatalogType ct : e.getValue()) {
                assert(ct.getCatalog() == getCatalog()) : ct.getPath() + " has wrong catalog";
                ct.validate();
            }
        }*/
    }
}

