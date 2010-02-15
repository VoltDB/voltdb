/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

/**
 * Instructions to the executor to execute part of an execution plan
 */
public class PlanFragment extends CatalogType {

    boolean m_hasdependencies;
    boolean m_multipartition;
    String m_plannodetree = new String();
    boolean m_nontransactional;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("hasdependencies", m_hasdependencies);
        m_fields.put("multipartition", m_multipartition);
        m_fields.put("plannodetree", m_plannodetree);
        m_fields.put("nontransactional", m_nontransactional);
    }

    void update() {
        m_hasdependencies = (Boolean) m_fields.get("hasdependencies");
        m_multipartition = (Boolean) m_fields.get("multipartition");
        m_plannodetree = (String) m_fields.get("plannodetree");
        m_nontransactional = (Boolean) m_fields.get("nontransactional");
    }

    /** GETTER: Dependencies must be received before this plan fragment can execute */
    public boolean getHasdependencies() {
        return m_hasdependencies;
    }

    /** GETTER: Should this plan fragment be sent to all partitions */
    public boolean getMultipartition() {
        return m_multipartition;
    }

    /** GETTER: A serialized representation of the plan-graph/plan-pipeline */
    public String getPlannodetree() {
        return m_plannodetree;
    }

    /** GETTER: True if this fragment doesn't read from or write to any persistent tables */
    public boolean getNontransactional() {
        return m_nontransactional;
    }

    /** SETTER: Dependencies must be received before this plan fragment can execute */
    public void setHasdependencies(boolean value) {
        m_hasdependencies = value; m_fields.put("hasdependencies", value);
    }

    /** SETTER: Should this plan fragment be sent to all partitions */
    public void setMultipartition(boolean value) {
        m_multipartition = value; m_fields.put("multipartition", value);
    }

    /** SETTER: A serialized representation of the plan-graph/plan-pipeline */
    public void setPlannodetree(String value) {
        m_plannodetree = value; m_fields.put("plannodetree", value);
    }

    /** SETTER: True if this fragment doesn't read from or write to any persistent tables */
    public void setNontransactional(boolean value) {
        m_nontransactional = value; m_fields.put("nontransactional", value);
    }

}
