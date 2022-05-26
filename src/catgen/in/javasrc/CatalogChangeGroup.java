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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.voltdb.catalog.CatalogDiffEngine.DiffClass;

/**
 * Describes the set of changes to a subtree of the catalog. For example, a {@link ChangeGroup}
 * could describe all of the changes to procedures in a catalog. This is purely used for
 * printing human readable summaries.
 */
class CatalogChangeGroup {

    /**
     * Given a type present in both catalogs, list the fields
     * that have changed between them. Together with the types
     * themselves, you can get the values of the fields before
     * and after.
     */
    static class FieldChange {
        CatalogType newType = null;
        CatalogType prevType = null;
        final Set<String> changedFields = new HashSet<String>();
    }

    /**
     * For a particular CatalogType instance, list child additions, deletions,
     * children with modified fields and personally modified fields.
     */
    static class TypeChanges {
        CatalogType typeInstance = null;

        // nodes added under a clz instance, mapped from their parent
        final List<CatalogType> typeAdditions = new ArrayList<CatalogType>();
        // nodes dropped from under a clz instance, mapped from their parent
        final List<CatalogType> typeDeletions = new ArrayList<CatalogType>();

        // all field changes for the clz instance
        final FieldChange typeChanges = new FieldChange();

        // all fields changed for the descendants of the parent clz instance
        // for each descendant with changed fields, store a list of all changed fields
        final Map<CatalogType, FieldChange> childChanges = new TreeMap<CatalogType, FieldChange>();
    }

    // the class of the base item in the subtree
    // for example, procedure
    final Class<?> clz;

    // nodes of type clz added
    List<CatalogType> groupAdditions = new ArrayList<CatalogType>();
    // nodes of type clz dropped
    List<CatalogType> groupDeletions = new ArrayList<CatalogType>();

    Map<CatalogType, TypeChanges> groupChanges = new TreeMap<CatalogType, TypeChanges>();

    CatalogChangeGroup(DiffClass diffClass) {
        clz = diffClass.clz;
    }

    void processAddition(CatalogType type) {
        if (type.getClass().equals(clz)) {
            groupAdditions.add(type);
            return;
        }
        CatalogType parent = type.getParent();
        while (parent.getClass().equals(clz) == false) {
            parent = parent.getParent();
        }

        TypeChanges metaChanges = groupChanges.get(parent);
        if (metaChanges == null) {
            metaChanges = new TypeChanges();
            metaChanges.typeInstance = parent;
            groupChanges.put(parent, metaChanges);
        }
        metaChanges.typeAdditions.add(type);
    }

    void processDeletion(CatalogType type, CatalogType newlyChildlessParent) {
        if (type.getClass().equals(clz)) {
            groupDeletions.add(type);
            return;
        }
        // need to use a parent from the new tree, not the old one
        CatalogType parent = newlyChildlessParent;
        while (parent.getClass().equals(clz) == false) {
            parent = parent.getParent();
        }

        TypeChanges metaChanges = groupChanges.get(parent);
        if (metaChanges == null) {
            metaChanges = new TypeChanges();
            metaChanges.typeInstance = parent;
            groupChanges.put(parent, metaChanges);
        }
        metaChanges.typeDeletions.add(type);
    }

    void processChange(CatalogType newType, CatalogType prevType, String field) {
        CatalogType parent = newType;
        while (parent.getClass().equals(clz) == false) {
            parent = parent.getParent();
        }

        TypeChanges metaChanges = groupChanges.get(parent);
        if (metaChanges == null) {
            metaChanges = new TypeChanges();
            metaChanges.typeInstance = parent;
            groupChanges.put(parent, metaChanges);
        }

        FieldChange fc = null;

        // object equality is intentional here
        if (parent == newType) {
            fc = metaChanges.typeChanges;
        }
        else {
            fc = metaChanges.childChanges.get(newType);
            if (fc == null) {
                fc = new FieldChange();
                metaChanges.childChanges.put(newType, fc);
            }
        }
        fc.newType = newType; // might be setting this twice, but no harm done
        fc.prevType = prevType; // ditto
        fc.changedFields.add(field);
    }
}
