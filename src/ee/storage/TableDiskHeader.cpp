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

#include "TableDiskHeader.h"

#include "catalog/cluster.h"
#include "catalog/database.h"
#include "catalog/host.h"
#include "catalog/partition.h"
#include "catalog/site.h"
#include "catalog/table.h"

#include <sstream>
#include "boost/shared_ptr.hpp"

using namespace voltdb;
using namespace std;

boost::shared_ptr<TableDiskHeader>
TableDiskHeader::constructHeaderOnSave(const catalog::Catalog& catalog,
                                       int32_t tableId, int32_t siteId)
{
    return boost::shared_ptr<TableDiskHeader>(new TableDiskHeader(catalog,
                                                                  tableId,
                                                                  siteId));
}

boost::shared_ptr<TableDiskHeader>
TableDiskHeader::constructHeaderOnRestore(std::istream& istream)
{
    boost::shared_ptr<TableDiskHeader> header(new TableDiskHeader());
    // XXX consider catching exception here and returning null?
    header->readHeader(istream);
    return header;
}

TableDiskHeader::TableDiskHeader(const catalog::Catalog& catalog,
                                 int32_t tableId, int32_t siteId)
{
    // Calling code in VoltDBEngine has already validated that the
    // tableGuid is in the catalog.  We'll assume (for now) that this
    // means that the rest of the catalog is valid.
    // XXX make me fail in a more recoverable manner later --izzy

    catalog::Cluster *cluster = catalog.clusters().get("cluster");
    assert(cluster);
    catalog::Database *db = cluster->databases().get("database");
    assert(db);

    catalog::Table* p_table = db->tables().getAtRelativeIndex(tableId);
    assert(p_table);

    catalog::Database* p_database =
        dynamic_cast<catalog::Database*>(p_table->parent());
    assert(p_database);

    catalog::Cluster* p_cluster =
        dynamic_cast<catalog::Cluster*>(p_database->parent());
    assert(p_cluster);

    stringstream site_name;
    site_name << siteId;
    catalog::Site* p_site = p_cluster->sites().get(site_name.str());
    assert(p_site);

    // Initialize the version number to all 0s as a placeholder for now
    for (int i = 0; i < 4; i++)
    {
        m_versionNum[i] = 0;
    }

    m_clusterName = p_cluster->name();
    m_databaseName = p_database->name();
    m_tableName = p_table->name();
    m_isReplicated = p_table->isreplicated();
    if (!m_isReplicated)
    {
        m_partitionId = atoi(p_site->partition()->name().c_str());
        m_totalPartitions = p_cluster->partitions().size();
    }
    m_siteId = atoi(p_site->name().c_str());
    m_hostId = atoi(p_site->host()->name().c_str());
}

TableDiskHeader::TableDiskHeader()
{
}

size_t
TableDiskHeader::getHeaderSize() const
{
    // XXX there's gotta be a better way to do this
    size_t header_size =
        // each string preceded by length as a short
        4 * sizeof(int32_t) + // sizeof version number storage
        sizeof(int32_t) + // sizeof serialized host ID
        sizeof(int32_t) + // sizeof serialized site ID
        3 * sizeof(int16_t) +
        m_clusterName.length() +
        m_databaseName.length() +
        m_tableName.length() +
        1; // isReplicated, bools serialize as one octet
    if (!m_isReplicated)
    {
        header_size +=
            sizeof(int32_t) + // sizeof serialized partition ID
            sizeof(int32_t); // sizeof serialized m_totalPartitions
    }
    return header_size;
}

int
TableDiskHeader::getHostId() const
{
    return m_hostId;
}

int
TableDiskHeader::getSiteId() const
{
    return m_siteId;
}

std::string
TableDiskHeader::getClusterName() const
{
    return m_clusterName;
}

std::string
TableDiskHeader::getDatabaseName() const
{
    return m_databaseName;
}

std::string
TableDiskHeader::getTableName() const
{
    return m_tableName;
}

bool
TableDiskHeader::isReplicated() const
{
    return m_isReplicated;
}

int
TableDiskHeader::getPartitionId() const
{
    assert(!m_isReplicated);
    return m_partitionId;
}

int
TableDiskHeader::getTotalPartitions() const
{
    assert(!m_isReplicated);
    return m_totalPartitions;
}

void
TableDiskHeader::writeHeader(std::ostream& ostream)
{
    // Add 4 octets to store the header size
    size_t buff_size = getHeaderSize() + 4;
    char* buff = new char[buff_size];
    ReferenceSerializeOutput rso(buff, buff_size);
    std::size_t pos = rso.position();
    rso.writeInt(-1); // placeholder for header length
    for (int i = 0; i < 4; i++)
    {
        rso.writeInt(m_versionNum[i]);
    }
    rso.writeInt(m_hostId);
    rso.writeInt(m_siteId);
    rso.writeTextString(m_clusterName);
    rso.writeTextString(m_databaseName);
    rso.writeTextString(m_tableName);
    rso.writeBool(m_isReplicated);
    if (!m_isReplicated)
    {
        rso.writeInt(m_partitionId);
        rso.writeInt(m_totalPartitions);
    }
    rso.writeIntAt(pos, static_cast<int32_t>(rso.position() - pos -
                                             sizeof(int32_t)));
    ostream.write(buff, buff_size);
    delete[] buff;
}

bool
TableDiskHeader::writeHeader(FILE *file)
{
    // Add 4 octets to store the header size
    size_t buff_size = getHeaderSize() + 4;
    char* buff = new char[buff_size];
    ReferenceSerializeOutput rso(buff, buff_size);
    std::size_t pos = rso.position();
    rso.writeInt(-1); // placeholder for header length
    for (int i = 0; i < 4; i++)
    {
        rso.writeInt(m_versionNum[i]);
    }
    rso.writeInt(m_hostId);
    rso.writeInt(m_siteId);
    rso.writeTextString(m_clusterName);
    rso.writeTextString(m_databaseName);
    rso.writeTextString(m_tableName);
    rso.writeBool(m_isReplicated);
    if (!m_isReplicated)
    {
        rso.writeInt(m_partitionId);
        rso.writeInt(m_totalPartitions);
    }
    rso.writeIntAt(pos, static_cast<int32_t>(rso.position() - pos -
                                             sizeof(int32_t)));
    std::size_t written = fwrite(buff, 1, buff_size, file);
    delete[] buff;
    if (written != buff_size) {
        return false;
    }
    return true;
}

void
TableDiskHeader::readHeader(istream& istream)
{
    char length_buff[sizeof(int32_t)];
    istream.read(length_buff, sizeof(int32_t));
    ReferenceSerializeInput rsi(length_buff, sizeof(int32_t));
    int header_length = rsi.readInt();

    char header_buff[header_length];
    istream.read(header_buff, header_length);
    ReferenceSerializeInput header_rsi(header_buff, header_length);
    for (int i = 0; i < 4; i++)
    {
        m_versionNum[i] = header_rsi.readInt();
    }
    m_hostId = header_rsi.readInt();
    m_siteId = header_rsi.readInt();
    m_clusterName = header_rsi.readTextString();
    m_databaseName = header_rsi.readTextString();
    m_tableName = header_rsi.readTextString();
    m_isReplicated = header_rsi.readBool();
    if (!m_isReplicated)
    {
        m_partitionId = header_rsi.readInt();
        m_totalPartitions = header_rsi.readInt();
    }

    assert(!m_clusterName.empty());
    assert(!m_databaseName.empty());
    assert(!m_tableName.empty());
}
