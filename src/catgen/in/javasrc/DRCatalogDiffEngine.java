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

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
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
    private static final VoltLogger s_log = new VoltLogger("DRAGENT");

    /* White list of fields that we care about for DR for table children classes.
       This is used only in serialize commands for DR method.
       There are duplicates added to the set because it lists all the fields per type */
    private static final Set<String> s_whiteListFields = ImmutableSet.of(
            /* Table */
            "isreplicated", "partitioncolumn", "materializer", "signature", "isDRed",
            /* ColumnRef */
            "index", "column",
            /* Column */
            "index", "type", "size", "nullable", "name", "inbytes",
            /* Index */
            "unique", "assumeUnique", "countable", "type", "expressionsjson", "predicatejson",
            /* Constraint */
            "type", "oncommit", "index", "foreignkeytable"
            );

    private static final Set<Class<? extends CatalogType>> s_whiteListChildren =
            ImmutableSet.of(Column.class, Index.class, Constraint.class, ColumnRef.class);

    private byte m_remoteClusterId;
    private final Set<String> m_replicableTables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    public DRCatalogDiffEngine(Catalog localCatalog, Catalog remoteCatalog, byte remoteClusterId) {
        super(localCatalog, remoteCatalog, false, false);
        for (Table table : CatalogUtil.getDatabase(localCatalog).getTables()) {
            assert table.getIsdred() : table + "is not DRed";
            m_replicableTables.add(table.getTypeName());
        }
        m_remoteClusterId = remoteClusterId;
        runDiff(localCatalog, remoteCatalog);
    }

    public static Catalog getDrCatalog(Catalog catalog) {
        String drCatalogCommands = getDrCatalogCommands(catalog);
        return executeDrCatalogCommands(drCatalogCommands);
    }

    public static DRCatalogCommands serializeCatalogCommandsForDr(Catalog catalog, int protocolVersion) {
        assert (protocolVersion == -1 || protocolVersion >= DRProtocol.MULTICLUSTER_PROTOCOL_VERSION);

        String catalogCommands = getDrCatalogCommands(catalog);
        PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(catalogCommands.getBytes(Constants.UTF8ENCODING));
        // DR catalog exchange still uses the old gzip scheme for now, next time DR protocol version is bumped
        // the logic can be updated to choose compression/decompression scheme based on agreed protocol version
        return new DRCatalogCommands(protocolVersion, crc.getValue(), Encoder.compressAndBase64Encode(catalogCommands));
    }

    private static String getDrCatalogCommands(Catalog catalog) {
        Cluster cluster = CatalogUtil.getCluster(catalog);
        CatalogSerializer serializer = new CatalogSerializer(s_whiteListFields, s_whiteListChildren);
        serializer.writeCommandForField(cluster, "drRole", true);

        Database db = CatalogUtil.getDatabase(catalog);
        for (Table t : db.getTables()) {
            if (t.getIsdred() && t.getMaterializer() == null && ! CatalogUtil.isStream(db, t)) {
                t.accept(serializer);
            }
        }
        return serializer.getResult();
    }

    public static Catalog deserializeCatalogCommandsForDr(String encodedCatalogCommands) {
        return deserializeCatalogCommandsForDr(Encoder.base64Decode(encodedCatalogCommands));
    }

    public static Catalog deserializeCatalogCommandsForDr(byte[] encodedCatalogCommands) {
        String catalogCommands = new String(Encoder.decompress(encodedCatalogCommands), Constants.UTF8ENCODING);
        return executeDrCatalogCommands(catalogCommands);
    }

    private static Catalog executeDrCatalogCommands(String drCatalogCommands) {
        Catalog deserializedMasterCatalog = new Catalog();
        Cluster c = deserializedMasterCatalog.getClusters().add("cluster");
        Database db = c.getDatabases().add("database");
        deserializedMasterCatalog.execute(drCatalogCommands);

        if (db.getIsactiveactivedred()) {
            // The catalog came from an old version, set DR role here
            // appropriately so that the diff engine can just look at the role.
            c.setDrrole(DrRoleType.XDCR.value());
        }

        return deserializedMasterCatalog;
    }

    public static String[] calculateReplicableTables(byte clusterId, byte[] remoteCatalogBytes) {
        Catalog drCatalog = getDrCatalog(VoltDB.instance().getCatalogContext().catalog);
        return calculateReplicableTables(clusterId, drCatalog, remoteCatalogBytes);
    }

    public static String[] calculateReplicableTables(byte clusterId, Catalog local, byte[] remoteCatalogBytes) {
        Catalog remote = deserializeCatalogCommandsForDr(remoteCatalogBytes);

        DRCatalogDiffEngine diff = new DRCatalogDiffEngine(local, remote, clusterId);

        if (!diff.supported()) {
            s_log.warn(diff.errors()
                    + "Inconsistent DR table schemas across clusters prevents replication of those tables.");
        }

        return diff.m_replicableTables.toArray(new String[diff.m_replicableTables.size()]);
    }

    @Override
    protected String checkAddDropWhitelist(final CatalogType suspect, final ChangeType changeType) {
        // Only on remote
        if (ChangeType.ADDITION == changeType && suspect instanceof Table) {
            assert ((Boolean)suspect.getField("isDRed"));
            return "Missing DR table " + suspect.getTypeName() + " on local cluster";
        }

        if (suspect instanceof Column || isUniqueIndex(suspect) || isUniqueIndexColumn(suspect)) {
            removeTableAncestor(suspect);
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
        } else if ((remoteType instanceof Index && isUniqueIndex(remoteType) == isUniqueIndex(localType))
                || (remoteType instanceof ColumnRef && !isUniqueIndexColumn(remoteType))) {
            return null;
        }
        removeTableAncestor(localType);
        return "Incompatible schema between remote cluster " + m_remoteClusterId + " and local cluster: field " + field
                + " in schema object " + remoteType;
    }

    /**
     * Remove the ancestor of {@code type} which is a {@link Table} from {@link #m_replicableTables}. {@code type} can
     * be a {@link Table}
     *
     * @param type {@link CatalogType} whose {@link Table} ancestor should be removed
     */
    private void removeTableAncestor(CatalogType type) {
        for (CatalogType ancestor = type; ancestor != null; ancestor = ancestor.getParent()) {
            if (ancestor instanceof Table) {
                m_replicableTables.remove(ancestor.getTypeName());
                return;
            }
        }

        throw new IllegalArgumentException("Type does not have a table as an ancestor: " + type);
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
