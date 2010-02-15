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
 * A parameterized SQL statement embedded in a stored procedure
 */
public class Statement extends CatalogType {

    String m_sqltext = new String();
    int m_querytype;
    boolean m_readonly;
    boolean m_singlepartition;
    boolean m_replicatedtabledml;
    boolean m_batched;
    int m_paramnum;
    CatalogMap<StmtParameter> m_parameters;
    CatalogMap<PlanFragment> m_fragments;
    CatalogMap<Column> m_output_columns;
    String m_exptree = new String();
    String m_fullplan = new String();
    int m_cost;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("sqltext", m_sqltext);
        m_fields.put("querytype", m_querytype);
        m_fields.put("readonly", m_readonly);
        m_fields.put("singlepartition", m_singlepartition);
        m_fields.put("replicatedtabledml", m_replicatedtabledml);
        m_fields.put("batched", m_batched);
        m_fields.put("paramnum", m_paramnum);
        m_parameters = new CatalogMap<StmtParameter>(catalog, this, path + "/" + "parameters", StmtParameter.class);
        m_childCollections.put("parameters", m_parameters);
        m_fragments = new CatalogMap<PlanFragment>(catalog, this, path + "/" + "fragments", PlanFragment.class);
        m_childCollections.put("fragments", m_fragments);
        m_output_columns = new CatalogMap<Column>(catalog, this, path + "/" + "output_columns", Column.class);
        m_childCollections.put("output_columns", m_output_columns);
        m_fields.put("exptree", m_exptree);
        m_fields.put("fullplan", m_fullplan);
        m_fields.put("cost", m_cost);
    }

    void update() {
        m_sqltext = (String) m_fields.get("sqltext");
        m_querytype = (Integer) m_fields.get("querytype");
        m_readonly = (Boolean) m_fields.get("readonly");
        m_singlepartition = (Boolean) m_fields.get("singlepartition");
        m_replicatedtabledml = (Boolean) m_fields.get("replicatedtabledml");
        m_batched = (Boolean) m_fields.get("batched");
        m_paramnum = (Integer) m_fields.get("paramnum");
        m_exptree = (String) m_fields.get("exptree");
        m_fullplan = (String) m_fields.get("fullplan");
        m_cost = (Integer) m_fields.get("cost");
    }

    /** GETTER: The text of the sql statement */
    public String getSqltext() {
        return m_sqltext;
    }

    public int getQuerytype() {
        return m_querytype;
    }

    /** GETTER: Can the statement modify any data? */
    public boolean getReadonly() {
        return m_readonly;
    }

    /** GETTER: Does the statement only use data on one partition? */
    public boolean getSinglepartition() {
        return m_singlepartition;
    }

    /** GETTER: Should the result of this statememt be divided by partition count before returned */
    public boolean getReplicatedtabledml() {
        return m_replicatedtabledml;
    }

    public boolean getBatched() {
        return m_batched;
    }

    public int getParamnum() {
        return m_paramnum;
    }

    /** GETTER: The set of parameters to this SQL statement */
    public CatalogMap<StmtParameter> getParameters() {
        return m_parameters;
    }

    /** GETTER: The set of plan fragments used to execute this statement */
    public CatalogMap<PlanFragment> getFragments() {
        return m_fragments;
    }

    /** GETTER: The set of columns in the output table */
    public CatalogMap<Column> getOutput_columns() {
        return m_output_columns;
    }

    /** GETTER: A serialized representation of the original expression tree */
    public String getExptree() {
        return m_exptree;
    }

    /** GETTER: A serialized representation of the un-fragmented plan */
    public String getFullplan() {
        return m_fullplan;
    }

    /** GETTER: The cost of this plan measured in arbitrary units */
    public int getCost() {
        return m_cost;
    }

    /** SETTER: The text of the sql statement */
    public void setSqltext(String value) {
        m_sqltext = value; m_fields.put("sqltext", value);
    }

    public void setQuerytype(int value) {
        m_querytype = value; m_fields.put("querytype", value);
    }

    /** SETTER: Can the statement modify any data? */
    public void setReadonly(boolean value) {
        m_readonly = value; m_fields.put("readonly", value);
    }

    /** SETTER: Does the statement only use data on one partition? */
    public void setSinglepartition(boolean value) {
        m_singlepartition = value; m_fields.put("singlepartition", value);
    }

    /** SETTER: Should the result of this statememt be divided by partition count before returned */
    public void setReplicatedtabledml(boolean value) {
        m_replicatedtabledml = value; m_fields.put("replicatedtabledml", value);
    }

    public void setBatched(boolean value) {
        m_batched = value; m_fields.put("batched", value);
    }

    public void setParamnum(int value) {
        m_paramnum = value; m_fields.put("paramnum", value);
    }

    /** SETTER: A serialized representation of the original expression tree */
    public void setExptree(String value) {
        m_exptree = value; m_fields.put("exptree", value);
    }

    /** SETTER: A serialized representation of the un-fragmented plan */
    public void setFullplan(String value) {
        m_fullplan = value; m_fields.put("fullplan", value);
    }

    /** SETTER: The cost of this plan measured in arbitrary units */
    public void setCost(int value) {
        m_cost = value; m_fields.put("cost", value);
    }

}
