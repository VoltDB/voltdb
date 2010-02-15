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
 * A set of connected hosts running one or more database application contexts
 */
public class Cluster extends CatalogType {

    CatalogMap<Database> m_databases;
    CatalogMap<Host> m_hosts;
    CatalogMap<Site> m_sites;
    CatalogMap<Partition> m_partitions;
    String m_leaderaddress = new String();
    int m_localepoch;
    boolean m_securityEnabled;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_databases = new CatalogMap<Database>(catalog, this, path + "/" + "databases", Database.class);
        m_childCollections.put("databases", m_databases);
        m_hosts = new CatalogMap<Host>(catalog, this, path + "/" + "hosts", Host.class);
        m_childCollections.put("hosts", m_hosts);
        m_sites = new CatalogMap<Site>(catalog, this, path + "/" + "sites", Site.class);
        m_childCollections.put("sites", m_sites);
        m_partitions = new CatalogMap<Partition>(catalog, this, path + "/" + "partitions", Partition.class);
        m_childCollections.put("partitions", m_partitions);
        m_fields.put("leaderaddress", m_leaderaddress);
        m_fields.put("localepoch", m_localepoch);
        m_fields.put("securityEnabled", m_securityEnabled);
    }

    void update() {
        m_leaderaddress = (String) m_fields.get("leaderaddress");
        m_localepoch = (Integer) m_fields.get("localepoch");
        m_securityEnabled = (Boolean) m_fields.get("securityEnabled");
    }

    /** GETTER: The set of databases the cluster is running */
    public CatalogMap<Database> getDatabases() {
        return m_databases;
    }

    /** GETTER: The set of host that belong to this cluster */
    public CatalogMap<Host> getHosts() {
        return m_hosts;
    }

    /** GETTER: The set of physical execution contexts executing on this cluster */
    public CatalogMap<Site> getSites() {
        return m_sites;
    }

    /** GETTER: The set of logical partitions in this cluster */
    public CatalogMap<Partition> getPartitions() {
        return m_partitions;
    }

    /** GETTER: The ip or hostname of the cluster 'leader' - see docs for details */
    public String getLeaderaddress() {
        return m_leaderaddress;
    }

    /** GETTER: The number of seconds since the epoch that we're calling our local epoch */
    public int getLocalepoch() {
        return m_localepoch;
    }

    /** GETTER: Whether security and authentication should be enabled/disabled */
    public boolean getSecurityenabled() {
        return m_securityEnabled;
    }

    /** SETTER: The ip or hostname of the cluster 'leader' - see docs for details */
    public void setLeaderaddress(String value) {
        m_leaderaddress = value; m_fields.put("leaderaddress", value);
    }

    /** SETTER: The number of seconds since the epoch that we're calling our local epoch */
    public void setLocalepoch(int value) {
        m_localepoch = value; m_fields.put("localepoch", value);
    }

    /** SETTER: Whether security and authentication should be enabled/disabled */
    public void setSecurityenabled(boolean value) {
        m_securityEnabled = value; m_fields.put("securityEnabled", value);
    }

}
