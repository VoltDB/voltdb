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

import java.util.HashMap;

/**
 * The root class in the Catalog hierarchy, which is essentially a tree of
 * instances of CatalogType objects, accessed by paths globally, and child
 * names when given a parent.
 */
public class Catalog extends CatalogType {

    private final HashMap<String, CatalogType> m_pathCache = new HashMap<String, CatalogType>();
    private CatalogType m_prevUsedPath = null;

    CatalogMap<Cluster> m_clusters;

    /**
     * Create a new Catalog hierarchy.
     */
    public Catalog() {
        setBaseValues(this, null, "/", "catalog");
        m_clusters = new CatalogMap<Cluster>(this, this, "/clusters", Cluster.class);
        m_childCollections.put("clusters", m_clusters);
        m_relativeIndex = 1;
    }

    /**
     * Run one or more single-line catalog commands separated by newlines.
     * See the docs for more info on catalog statements.
     * @param commands A string containing one or more catalog commands separated by
     * newlines
     */
    public void execute(final String commands) {

        int ctr = 0;
        for (String line : commands.split("\n")) {
            try {
                if (line.length() > 0) executeOne(line);
            }
            catch (Exception ex) {
                String msg = "Invalid catalog command on line " + ctr + "\n" +
                    "Contents: '" + line + "'\n";
                ex.printStackTrace();
                throw new RuntimeException(msg, ex);

            }
            ctr++;
        }
    }

    void executeOne(String stmt) {
        stmt = stmt.trim();

        // command comes before the first space (add or set)
        int pos = stmt.indexOf(' ');
        assert pos != -1;
        String cmd = stmt.substring(0, pos);
        stmt = stmt.substring(pos + 1);

        // ref to a catalog node between first two spaces
        pos = stmt.indexOf(' ');
        assert pos != -1;
        String ref = stmt.substring(0, pos);
        stmt = stmt.substring(pos + 1);

        // spaces 2 & 3 separate the two arguments
        pos = stmt.indexOf(' ');
        assert pos != -1;
        String arg1 = stmt.substring(0, pos);
        String arg2 = stmt.substring(pos + 1);

        // resolve the ref to a node in the catalog
        CatalogType resolved = null;
        if (ref.equals("$PREV")) {
            if (m_prevUsedPath == null)
                throw new CatalogException("$PREV reference was not preceded by a cached reference.");
            resolved = m_prevUsedPath;
        }
        else {
            resolved = getItemForRef(ref);
            if (resolved == null) {
                throw new CatalogException("Unable to find reference for catalog item '" + ref + "'");
            }
            m_prevUsedPath = resolved;
        }

        // run either command
        if (cmd.equals("add")) {
            resolved.addChild(arg1, arg2);
        }
        else if (cmd.equals("delete")) {
            resolved.delete(arg1, arg2);
            String toDelete = ref + "/" + arg1 + "[" + arg2 + "]";
            CatalogType thing = m_pathCache.remove(toDelete);
            if (thing == null) {
                throw new CatalogException("Unable to find reference to delete: " + toDelete);
            }
        }
        else if (cmd.equals("set")) {
            resolved.set(arg1, arg2);
        }
    }

    CatalogType getItemForRef(final String ref) {
        // if it's a path
        return m_pathCache.get(ref);
    }

    CatalogType getItemForPath(CatalogType parent, final String path) {
        // remove the starting slash
        String realpath = path;
        if (path.startsWith("/"))
            realpath = path.substring(1);

        String[] parts = realpath.split("/", 2);
        // root case
        if (parts[0].length() == 0)
            return this;
        // child of root
        if (parts.length == 1)
            return getItemForPathPart(parent, parts[0]);

        // recursive case
        CatalogType nextParent = getItemForPathPart(parent, parts[0]);
        if (nextParent == null)
            throw new CatalogException("couldn't find next child in path.");
        return getItemForPath(nextParent, parts[1]);
    }

    CatalogType getItemForPathPart(CatalogType parent, String path) {
        String[] parts = path.split("\\[", 2);
        parts[1] = parts[1].split("\\]", 2)[0];
        return parent.getChild(parts[0], parts[1]);
    }

    void registerGlobally(CatalogType x) {
        m_pathCache.put(x.m_path, x);
    }

    /**
     * Serialize the catalog to a string representation. This actually
     * creates a set of catalog commands which, re-run in order on an
     * empty catalog, will recreate this catalog exactly.
     * @return The serialized string representation of the catalog.
     */
    public String serialize() {
        StringBuilder sb = new StringBuilder();

        writeFieldCommands(sb);
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

    @Override
    void update() {
        // does nothing
    }

    /** GETTER: The set of the clusters in this catalog */
    public CatalogMap<Cluster> getClusters() {
        return m_clusters;
    }
}
