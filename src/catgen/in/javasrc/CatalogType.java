/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;

/**
 * The base class for all objects in the Catalog. CatalogType instances all
 * have a name and a path (from the root). They have fields and children.
 * All fields are simple types. All children are CatalogType instances.
 *
 */
public abstract class CatalogType implements Comparable<CatalogType> {

    static class UnresolvedInfo {
        String path;
    }

    LinkedHashMap<String, Object> m_fields = new LinkedHashMap<String, Object>();
    LinkedHashMap<String, CatalogMap<? extends CatalogType>> m_childCollections
        = new LinkedHashMap<String, CatalogMap<? extends CatalogType>>();

    String m_path;
    String m_typename;
    CatalogType m_parent;
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
    public String getPath() {
        return m_path;
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
        return m_parent;
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
    public Set<String> getFields() {
        return m_fields.keySet();
    }

    /**
     * Get the value of a field knowing only the name of the field
     * @param field The name of the field being requested
     * @return The field requested or null
     */
    public Object getField(String field) {
        Object ret = null;
        if (m_fields.containsKey(field)) {
            ret = m_fields.get(field);
            if (ret instanceof UnresolvedInfo) {
                return resolve(field, ((UnresolvedInfo) ret).path);
            }
        }
        return ret;
    }

    CatalogType resolve(String field, String path) {
        CatalogType retval = m_catalog.getItemForRef(path);
        m_fields.put(field, retval);
        return retval;
    }

    /**
     * This should only ever be called from CatalogMap.add(); it's my lazy hack
     * to avoid using reflection to instantiate records.
     */
    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        if ((name == null) || (catalog == null)) {
            throw new CatalogException("Null value where it shouldn't be.");
        }
        m_catalog = catalog;
        m_parent = parent;
        m_path = path;
        m_typename = name;
        catalog.registerGlobally(this);
    }

    abstract void update();

    CatalogType addChild(String collectionName, String childName) {
        CatalogMap<? extends CatalogType> map = m_childCollections.get(collectionName);
        if (map == null)
            throw new CatalogException("No collection for name");
        return map.add(childName);
    }

    CatalogType getChild(String collectionName, String childName) {
        CatalogMap<? extends CatalogType> map = m_childCollections.get(collectionName);
        if (map == null)
            return null;
        return map.get(childName);
    }

    void set(String field, String value) {
        if ((field == null) || (value == null)) {
            throw new CatalogException("Null value where it shouldn't be.");
        }

        if (m_fields.containsKey(field) == false)
            throw new CatalogException("Unexpected field name '" + field + "' for " + this);
        Object current = m_fields.get(field);

        value = value.trim();

        // handle refs
        if (value.startsWith("/")) {
            UnresolvedInfo uinfo = new UnresolvedInfo();
            uinfo.path = value;
            m_fields.put(field, uinfo);
        }
        // null refs
        else if (value.startsWith("null")) {
            m_fields.put(field, null);
        }
        // handle booleans
        else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            if ((current != null) && (current.getClass() != Boolean.class)) {
                throw new CatalogException("Unexpected type for field '" + field + "'.");
            }
            m_fields.put(field, Boolean.parseBoolean(value));
        }
        // handle strings
        else if ((value.startsWith("\"") && value.endsWith("\"")) ||
            (value.startsWith("'") && value.endsWith("'"))) {
            // ignoring null types here is sketch-city, but other options seem worse?
            if ((current != null) && (current.getClass() != String.class)) {
                throw new CatalogException("Unexpected type for field.");
            }
            value = value.substring(1, value.length() - 1);
            m_fields.put(field, value);
        }
        // handle ints
        else {
            boolean isint = value.length() > 0;
            for (int i = 0; i < value.length(); i++) {
                if ((i == 0) && (value.length() > 1) && (value.charAt(i) == '-'))
                    continue;
                if (!Character.isDigit(value.charAt(i)))
                    isint = false;
            }
            if (isint) {
                if ((current != null) && (current.getClass() != Integer.class)) {
                    throw new CatalogException("Unexpected type for field.");
                }
                int intValue = Integer.parseInt(value);
                m_fields.put(field, intValue);
            }
            // error
            else {
                throw new CatalogException("Unexpected non-digit character in '" + value + "' for field '" + field + "'");
            }
        }

        update();
    }

    void delete(String collectionName, String childName) {
        if ((collectionName == null) || (childName == null)) {
            throw new CatalogException("Null value where it shouldn't be.");
        }

        if (m_childCollections.containsKey(collectionName) == false)
            throw new CatalogException("Unexpected collection name '" + collectionName + "' for " + this);
        CatalogMap<? extends CatalogType> collection = m_childCollections.get(collectionName);

        collection.delete(childName);
    }

    void writeCreationCommand(StringBuilder sb) {
        // skip root node command
        if (m_path.equals("/"))
            return;

        int lastSlash = m_path.lastIndexOf("/");
        String key = m_path.substring(lastSlash + 1);
        String newPath = m_path.substring(0, lastSlash);
        if (newPath.length() == 0)
            newPath = "/";
        String[] parts = key.split("\\[");
        parts[1] = parts[1].substring(0, parts[1].length() - 1);
        parts[1] = parts[1].trim();

        sb.append("add ").append(newPath).append(" ");
        sb.append(parts[0]).append(" ").append(parts[1]);
        sb.append("\n");
    }

    void writeCommandForField(StringBuilder sb, String field, boolean printFullPath) {
        String path = m_path;
        if (!printFullPath) path = "$PREV"; // use cacheing to shrink output + speed parsing

        sb.append("set ").append(path).append(" ");
        sb.append(field).append(" ");
        Object value = m_fields.get(field);
        if (value == null) {
            if ((field.equals("partitioncolumn")) && (m_path.equals("/clusters[cluster]/databases[database]/procedures[delivery]")))
                System.out.printf("null for field %s at path %s\n", field, getPath());
            sb.append("null");

        }
        else if (value.getClass() == Integer.class)
            sb.append(value);
        else if (value.getClass() == Boolean.class)
            sb.append(Boolean.toString((Boolean)value));
        else if (value.getClass() == String.class)
            sb.append("\"").append(value).append("\"");
        else if (value instanceof CatalogType)
            sb.append(((CatalogType)value).getPath());
        else if (value instanceof UnresolvedInfo)
            sb.append(((UnresolvedInfo)value).path);
        else
            throw new CatalogException("Unsupported field type '" + value + "'");
        sb.append("\n");
    }

    void writeFieldCommands(StringBuilder sb) {
        int i = 0;
        for (String field : m_fields.keySet()) {
            writeCommandForField(sb, field, i == 0);
            ++i;
        }
    }

    void writeChildCommands(StringBuilder sb) {
        for (String childCollection : m_childCollections.keySet()) {
            CatalogMap<? extends CatalogType> map = m_childCollections.get(childCollection);
            map.writeCommandsForMembers(sb);
        }
    }

    @Override
    public int compareTo(CatalogType o) {
        if (this == o) {
            return 0;
        }
        // null comparands will throw an exception here:
        return getPath().compareTo(o.getPath());
    }

    CatalogType deepCopy(Catalog catalog, CatalogType parent) {

        CatalogType copy = null;
        try {
            copy = getClass().newInstance();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        assert(parent.getCatalog() == catalog);
        copy.setBaseValues(catalog, parent, m_path, m_typename);
        copy.m_relativeIndex = m_relativeIndex;

        for (Entry<String, Object> e : m_fields.entrySet()) {
            Object value = e.getValue();
            if (value instanceof CatalogType) {
                CatalogType type = (CatalogType) e.getValue();
                UnresolvedInfo uinfo = new UnresolvedInfo();
                uinfo.path = type.getPath();
                value = uinfo;
            }

            copy.m_fields.put(e.getKey(), value);
        }

        for (Entry<String, CatalogMap<? extends CatalogType>> e : m_childCollections.entrySet()) {
            CatalogMap<? extends CatalogType> mapCopy = copy.m_childCollections.get(e.getKey());
            mapCopy.copyFrom(e.getValue());
        }

        copy.update();
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
        CatalogType other = (CatalogType)obj;

        // are the fields the same value?
        if (m_fields.size() != other.m_fields.size())
            return false;
        for (String field : m_fields.keySet()) {
            if (m_fields.get(field) == null) {
                if (other.m_fields.get(field) != null)
                    return false;
            }
            else if (m_fields.get(field).equals(other.m_fields.get(field)) == false)
                return false;
        }

        // are the children the same (deep compare)
        for (String collectionName : m_childCollections.keySet()) {
            CatalogMap<? extends CatalogType> myMap = m_childCollections.get(collectionName);
            CatalogMap<? extends CatalogType> otherMap = m_childCollections.get(collectionName);

            // if two types are the same class, this shouldn't happen
            assert(otherMap != null);

            if (myMap.equals(otherMap) == false)
                return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = 1;
        // Generate something reasonably unique but consistent for this element
        result = 37 * result + m_path.hashCode();
        result = 31 * result + m_typename.hashCode();
        return result;
    }

    /**
     * Fails an assertion if any child of this object doesn't think
     * it's part of the same catalog.
     */
    public void validate() {
        for (Entry<String, Object> e : m_fields.entrySet()) {
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
        }
    }
}

