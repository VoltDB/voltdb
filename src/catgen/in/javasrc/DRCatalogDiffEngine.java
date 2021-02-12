/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
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

import java.util.List;
import java.util.Set;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.voltdb.common.Constants;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.dr2.DRProtocol;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.collect.ImmutableSet;

/**
 * Specialized CatalogDiffEngine that checks the following conditions:
 * - The localCatalog contains all DR tables contained in the remote catalog
 * - All shared DR tables contain the same columns in the same order
 * - All shared DR tables have the same partition column
 * - All shared DR tables have the same unique indexes/primary keys
 */
public class DRCatalogDiffEngine extends CatalogDiffEngine {
    /* White list of fields that we care about for DR for table children classes.
       This is used only in serialize commands for DR method.
       There are duplicates added to the set because it lists all the fields per type */
    private static final Set<String> s_whiteListFields = ImmutableSet.of(
            /* Table */
            "isreplicated", "partitioncolumn", "materializer", "signature", "tuplelimit", "isDRed",
            /* ColumnRef */
            "index", "column",
            /* Column */
            "index", "type", "size", "nullable", "name", "defaultvalue", "defaulttype",
            "aggregatetype", "matviewsource", "matview", "inbytes",
            /* Index */
            "unique", "assumeUnique", "countable", "type", "expressionsjson", "predicatejson",
            /* Constraint */
            "type", "oncommit", "index", "foreignkeytable",
            /* Statement */
            "sqltext", "querytype", "readonly", "singlepartition", "replicatedtabledml",
            "iscontentdeterministic", "isorderdeterministic", "nondeterminismdetail", "cost",
            "seqscancount", "explainplan", "tablesread", "tablesupdated", "indexesused", "cachekeyprefix"
            );

    private static final Set<Class<? extends CatalogType>> s_whiteListChildren =
            ImmutableSet.of(Column.class, Index.class, Constraint.class, Statement.class, ColumnRef.class);

    private byte m_remoteClusterId;

    public DRCatalogDiffEngine(Catalog localCatalog, Catalog remoteCatalog, byte remoteClusterId) {
        super(localCatalog, remoteCatalog);
        m_remoteClusterId = remoteClusterId;
    }

    public static DRCatalogCommands serializeCatalogCommandsForDr(Catalog catalog, int protocolVersion) {
        Cluster cluster = CatalogUtil.getCluster(catalog);
        CatalogSerializer serializer = new CatalogSerializer(s_whiteListFields, s_whiteListChildren);

        assert (protocolVersion == -1 || protocolVersion >= DRProtocol.MULTICLUSTER_PROTOCOL_VERSION);
        serializer.writeCommandForField(cluster, "drRole", true);

        Database db = CatalogUtil.getDatabase(catalog);
        for (Table t : db.getTables()) {
            if (t.getIsdred() && t.getMaterializer() == null && ! CatalogUtil.isStream(db, t)) {
                t.accept(serializer);
            }
        }
        String catalogCommands = serializer.getResult();
        PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(catalogCommands.getBytes(Constants.UTF8ENCODING));
        // DR catalog exchange still uses the old gzip scheme for now, next time DR protocol version is bumped
        // the logic can be updated to choose compression/decompression scheme based on agreed protocol version
        return new DRCatalogCommands(protocolVersion, crc.getValue(), Encoder.compressAndBase64Encode(catalogCommands));
    }

    public static Catalog deserializeCatalogCommandsForDr(String encodedCatalogCommands) {
        String catalogCommands = Encoder.decodeBase64AndDecompress(encodedCatalogCommands);
        Catalog deserializedMasterCatalog = new Catalog();
        Cluster c = deserializedMasterCatalog.getClusters().add("cluster");
        Database db = c.getDatabases().add("database");
        deserializedMasterCatalog.execute(catalogCommands);

        if (db.getIsactiveactivedred()) {
            // The catalog came from an old version, set DR role here
            // appropriately so that the diff engine can just look at the role.
            c.setDrrole(DrRoleType.XDCR.value());
        }

        return deserializedMasterCatalog;
    }

    @Override
    protected String checkAddDropWhitelist(final CatalogType suspect, final ChangeType changeType) {
        // Only on remote
        if (ChangeType.ADDITION == changeType && suspect instanceof Table) {
            assert ((Boolean)suspect.getField("isDRed"));
            return "Missing DR table " + suspect.getTypeName() + " on local cluster";
        }

        if (suspect instanceof Column || isUniqueIndex(suspect) || isUniqueIndexColumn(suspect)) {
            return "Missing " + suspect + " from " + suspect.getParent() + " on " +
                (ChangeType.ADDITION == changeType ? "local cluster" : "remote cluster " + m_remoteClusterId);
        }
        return null;
    }

    @Override
    protected String checkModifyWhitelist(final CatalogType remoteType, final CatalogType localType,
            final String field) {
        if (remoteType instanceof Cluster ||
            remoteType instanceof PlanFragment ||
            remoteType instanceof MaterializedViewInfo) {
            if ("drRole".equalsIgnoreCase(field)) {
                if (((String) localType.getField(field)).equalsIgnoreCase(DrRoleType.XDCR.value()) ^
                        ((String) remoteType.getField(field)).equalsIgnoreCase(DrRoleType.XDCR.value())) {
                    m_replicableTables.clear();
                    return "Incompatible DR modes between two clusters";
                }
            }
            return null;
        } else if (remoteType instanceof Table) {
            if ("estimatedtuplecount".equals(field) ||
                    "tuplelimit".equals(field) ||
                    "tableType".equals(field)) {
                return null;
            }
        } else if (remoteType instanceof Database) {
            if ("schema".equalsIgnoreCase(field) ||
                "securityprovider".equalsIgnoreCase(field) ||
                "isActiveActiveDRed".equalsIgnoreCase(field)) {
                return null;
            }
        } else if (remoteType instanceof Column) {
            if ("defaultvalue".equals(field) ||
                "defaulttype".equals(field) ||
                "matview".equals(field) ||
                "aggregatetype".equals(field) ||
                "matviewsource".equals(field)) {
                return null;
            }
        } else if (isTableLimitDeleteStmt(remoteType)) {
            return null;
        } else if ((remoteType instanceof Index && isUniqueIndex(remoteType) == isUniqueIndex(localType))
                || (remoteType instanceof ColumnRef && !isUniqueIndexColumn(remoteType))) {
            return null;
        }
        return "Incompatible schema between remote cluster " + m_remoteClusterId + " and local cluster: field " + field
                + " in schema object " + remoteType;
    }

    private boolean isUniqueIndexColumn(CatalogType suspect) {
        return suspect instanceof ColumnRef && isUniqueIndex(suspect.getParent());
    }

    private boolean isUniqueIndex(CatalogType suspect) {
        return suspect instanceof Index && (Boolean)suspect.getField("unique");
    }

    @Override
    protected TablePopulationRequirements checkAddDropIfTableIsEmptyWhitelist(final CatalogType suspect, final ChangeType changeType) {
        return null;
    }

    @Override
    public List<TablePopulationRequirements> checkModifyIfTableIsEmptyWhitelist(CatalogType suspect, CatalogType prevType, String field) {
        return null;
    }
}
