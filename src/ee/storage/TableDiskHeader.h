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

#ifndef TABLEDISKHEADER_H_
#define TABLEDISKHEADER_H_

#include "table.h"

#include "catalog/catalog.h"

#include <stdio.h>
#include <iostream>
#include <string>

namespace voltdb
{
    class TableDiskHeader
    {
    public:
        /**
         * Factory method to construct the disk file header for a
         * table using a pointer to that table.  Used for saving the
         * table to disk.
         *
         * @param catalog a const ref to the VoltDB catalog
         * @param tableId the index in the catalog of the table to be saved
         * @param siteId the id in the catalog of the site saving the table
         * @return a shared_ptr with a configured TableDiskHeader object
         */
        static boost::shared_ptr<TableDiskHeader>
            constructHeaderOnSave(const catalog::Catalog& catalog,
                                  int32_t tableId,
                                  int32_t siteId);

        /**
         * Factory method to construct the disk file header for a
         * table using an istream to the saved table on disk.  Used
         * for restoring the table from disk.
         *
         * @param istream an istream to the serialized saved table
         * @return a shared_ptr with a configured TableDiskHeader object
         */
        static boost::shared_ptr<TableDiskHeader>
            constructHeaderOnRestore(std::istream& istream);

        /**
         * get the size of the table savefile header containing table
         * meta-data.
         *
         * @return the savefile header size in octets.  Doesn't
         * currently include the 4 octets prepended to the header that
         * actually contain this value.
         */
        size_t getHeaderSize() const;

        /**
         * Get the host ID which this table had when it was saved
         *
         * @return the host id
         */
        int getHostId() const;

        /**
         * Get the site ID which this table had when it was saved
         *
         * @return the site id
         */
        int getSiteId() const;

        /**
         * Get the name of the cluster that this table is/was part of
         * when saved
         *
         * @return the cluster name
         */
        std::string getClusterName() const;

        /**
         * Get the name of the database that this table is/was part of
         * when saved
         *
         * @return the database name
         */
        std::string getDatabaseName() const;

        /**
         * Get the name of the table
         *
         * @return the table name
         */
        std::string getTableName() const;

        /**
         * Was this a replicated table when it was saved to disk?
         *
         * @return true if the table was replicated, false if not
         */
        bool isReplicated() const;

        /**
         * Get the partition ID which this table had when it was saved
         *
         * @return the partition id
         */
        int getPartitionId() const;

        /**
         * Get the total number of partitions for this table at the
         * time it was saved.
         *
         * @return the total partitions
         */
        int getTotalPartitions() const;

        /**
         * serialize the header and write it to the specified ostream
         *
         * @param ostream the C++ ostream to which to write the header
         */
        void writeHeader(std::ostream& ostream);

        /**
         * serialize the header and write it to the specified FILE handle
         *
         * @param file the C file to write the header to
         */
        bool writeHeader(FILE *file);

        /**
         * read the header from the specified istream and deserialize it
         *
         * @param istream the C++ ostream to which to read the header
         */
        void readHeader(std::istream& istream);

    private:
        TableDiskHeader(const catalog::Catalog& catalog, CatalogId tableId,
                        CatalogId siteId);

        TableDiskHeader();

        int m_versionNum[4];
        int m_hostId;
        int m_siteId;
        std::string m_clusterName;
        std::string m_databaseName;
        std::string m_tableName;
        bool m_isReplicated;
        int m_partitionId;
        int m_totalPartitions;
    };
}

#endif
