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
public class Catalog extends CatalogType {

    public static final char MAP_SEPARATOR = '#';

    //private final HashMap<String, CatalogType> m_pathCache = new HashMap<String, CatalogType>();
    //private final PatriciaTrie<CatalogType> m_pathCache = new PatriciaTrie<>();
    Cache<String, CatalogType> m_pathCache = CacheBuilder.newBuilder().maximumSize(8).build();

    private CatalogType m_prevUsedPath = null;

    CatalogMap<Cluster> m_clusters;

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

    /**
     * Run one or more single-line catalog commands separated by newlines.
     * See the docs for more info on catalog statements.
     * @param commands A string containing one or more catalog commands separated by
     * newlines
     */
    public void execute(final String commands) {
        String[] lines = commands.split("\n");

        for (int ctr = 0; ctr < lines.length; ctr++) {
            String line = lines[ctr];
            try {
                if (line.length() > 0) executeOne(line);
            }
            catch (Exception ex) {
                String msg = "Invalid catalog command on line " + ctr + "\n" +
                        "Contents: '" + line + "'\n";
                throw new RuntimeException(msg, ex);
            }
        }
    }

    public static class CatalogCmd {
        public char cmd;
        public String path;
        public String arg1;
        public String arg2;

        public CatalogCmd(char c, String p, String a1, String a2) {
            cmd = c;
            path = p;
            arg1 = a1;
            arg2 = a2;
        }

        public boolean isProcedureRelatedCmd() {
            if (path.indexOf("procedures#") != -1) return true;
            if ("procedures".equals(arg1) && path.endsWith("#database")) return true;

            return false;
        }
    }

    public static char parseStmtCmd(String stmt) {
        // command comes before the first space (add or set)
        int pos = 0;
        while (Character.isWhitespace(stmt.charAt(pos))) {
            ++pos;
        }
        return stmt.charAt(pos++);
    }

    public static CatalogCmd parseStmt(String stmt) {
        // command comes before the first space (add or set)
        int pos = 0;
        while (Character.isWhitespace(stmt.charAt(pos))) {
            ++pos;
        }
        char cmd = stmt.charAt(pos++);
        while (stmt.charAt(pos++) != ' ');

        // ref to a catalog node between first two spaces
        int refStart = pos;
        while (stmt.charAt(pos++) != ' ');
        String ref = stmt.substring(refStart, pos - 1);

        // spaces 2 & 3 separate the two arguments
        int argStart = pos;
        while (stmt.charAt(pos++) != ' ');
        String arg1 = stmt.substring(argStart, pos - 1);
        String arg2 = stmt.substring(pos);

        return new CatalogCmd(cmd, ref, arg1, arg2);
    }

    void executeOne(String stmt) {
        CatalogCmd catCmd = parseStmt(stmt);
        char cmd = catCmd.cmd;
        String path = catCmd.path;
        String arg1 = catCmd.arg1;
        String arg2 = catCmd.arg2;

        // resolve the ref to a node in the catalog
        CatalogType resolved = null;
        if (path.startsWith("$")) { // $PREV
            if (m_prevUsedPath == null) {
                throw new CatalogException("$PREV reference was not preceded by a cached reference.");
            }
            resolved = m_prevUsedPath;
        }
        else {
            resolved = getItemForPath(path);
            if (resolved == null) {
                throw new CatalogException("Unable to find reference for catalog item '" + path + "'");
            }
            m_prevUsedPath = resolved;
        }

        // run either command
        if (cmd == 'a') { // add
            resolved.getCollection(arg1).add(arg2);
        }
        else if (cmd == 'd') { // delete
            resolved.getCollection(arg1).delete(arg2);
            String toDelete = path + "/" + arg1 + MAP_SEPARATOR + arg2;
            m_pathCache.invalidate(toDelete);
        }
        else if (cmd == 's') { // set
            resolved.set(arg1, arg2);
        }
    }

    CatalogType getItemForPath(final String path) {
        // check the cache
        CatalogType retval = m_pathCache.getIfPresent(path);
        if (retval != null) return retval;

        int index = path.lastIndexOf('/');
        if (index == -1) {
            return getItemForPathPart(this, path);
        }

        // recursive case
        String immediateParentPath = path.substring(0, index);
        String subPath = path.substring(index);

        CatalogType immediateParent = getItemForPath(immediateParentPath);
        if (immediateParent == null) {
            return null;
        }
        // cache all parents
        m_pathCache.put(immediateParentPath, immediateParent);

        return getItemForPathPart(immediateParent, subPath);
    }

    static CatalogType getItemForPathPart(CatalogType parent, String path) {
        if (path.length() == 0) return parent;

        boolean hasStartSlash = path.charAt(0) == '/';

        if ((path.length() == 1) && hasStartSlash) return parent;

        int index = path.lastIndexOf(MAP_SEPARATOR);

        String collection = path.substring(hasStartSlash ? 1 : 0, index);
        String name = path.substring(index + 1, path.length());

        return parent.getCollection(collection).get(name);
    }

    /**
     * Serialize the catalog to a string representation. This actually
     * creates a set of catalog commands which, re-run in order on an
     * empty catalog, will recreate this catalog exactly.
     * @return The serialized string representation of the catalog.
     */
    public String serialize() {
        StringBuilder sb = new StringBuilder();

        writeFieldCommands(sb, null);
        writeChildCommands(sb);

        return sb.toString();
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

    @Override
    void writeCreationCommand(StringBuilder sb) {
        return;
    }
}
