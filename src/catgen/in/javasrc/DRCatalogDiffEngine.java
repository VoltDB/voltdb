/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import com.google_voltpatches.common.collect.Sets;
import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.voltdb.common.Constants;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.dr2.DRProtocol;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

/**
 * Specialized CatalogDiffEngine that checks the following conditions:
 * - The localCatalog contains all DR tables contained in the remote catalog
 * - All shared DR tables contain the same columns in the same order
 * - All shared DR tables have the same partition column
 * - All shared DR tables have the same unique indexes/primary keys
 */
public class DRCatalogDiffEngine extends CatalogDiffEngine {
    public DRCatalogDiffEngine(Catalog localCatalog, Catalog remoteCatalog) {
        super(localCatalog, remoteCatalog);
    }

    public static DRCatalogCommands serializeCatalogCommandsForDr(Catalog catalog, int protocolVersion) {
        Cluster cluster = catalog.getClusters().get("cluster");
        Database db = cluster.getDatabases().get("database");
        StringBuilder sb = new StringBuilder();

        if (protocolVersion == -1 || protocolVersion >= DRProtocol.MUTLICLUSTER_PROTOCOL_VERSION) {
            cluster.writeCommandForField(sb, "drRole", true);
        } else {
            // The compatibility mode will not understand the new drRole field,
            // so use the old field name. We'll remove this in v7.1 when the
            // compatibility mode is deprecated.
            db.writeCommandForField(sb, "isActiveActiveDRed", true);
        }

        for (Table t : db.getTables()) {
            if (t.getIsdred() && t.getMaterializer() == null && !CatalogUtil.isTableExportOnly(db, t)) {
                t.writeCreationCommand(sb);
                t.writeFieldCommands(sb);
                t.writeChildCommands(sb, Sets.newHashSet(Column.class, Index.class, Constraint.class, Statement.class));
            }
        }
        String catalogCommands = sb.toString();
        PureJavaCrc32 crc = new PureJavaCrc32();
        crc.update(catalogCommands.getBytes(Constants.UTF8ENCODING));
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
        if (ChangeType.ADDITION == changeType && suspect instanceof Table) {
            assert ((Boolean)suspect.getField("isDRed"));
            return "Missing DR table " + suspect.getTypeName() + " on replica cluster";
        }
        if (suspect instanceof Column || isUniqueIndex(suspect) || isUniqueIndexColumn(suspect)) {
            return "Missing " + suspect + " from " + suspect.getParent() + " on " + (ChangeType.ADDITION == changeType ? "replica" : "master");
        }
        return null;
    }

    @Override
    protected String checkModifyWhitelist(final CatalogType suspect, final CatalogType prevType, final String field) {
        if (suspect instanceof Cluster ||
            suspect instanceof PlanFragment ||
            suspect instanceof MaterializedViewInfo) {
            if ("drRole".equalsIgnoreCase(field)) {
                if (((String) prevType.getField(field)).equalsIgnoreCase(DrRoleType.XDCR.value()) ^
                    ((String) suspect.getField(field)).equalsIgnoreCase(DrRoleType.XDCR.value())) {
                    return "Incompatible DR modes between two clusters";
                }
            }
            return null;
        } else if (suspect instanceof Table) {
            if ("isdred".equalsIgnoreCase(field)) {
                assert ((Boolean)suspect.getField("isDRed"));
                return "Table " + suspect.getTypeName() + " has DR enabled on the master but not the replica";
            }
            if ("estimatedtuplecount".equals(field)) {
                return null;
            }
            if ("tuplelimit".equals(field)) {
                return null;
            }
        } else if (suspect instanceof Database) {
            if ("schema".equalsIgnoreCase(field) ||
                "securityprovider".equalsIgnoreCase(field) ||
                "isActiveActiveDRed".equalsIgnoreCase(field)) {
                return null;
            }
        } else if (suspect instanceof Column) {
            if ("defaultvalue".equals(field) ||
                "defaulttype".equals(field) ||
                "matview".equals(field) ||
                "aggregatetype".equals(field) ||
                "matviewsource".equals(field)) {
                return null;
            }
        } else if (isTableLimitDeleteStmt(suspect)) {
            return null;
        } else if ((suspect instanceof Index && isUniqueIndex(suspect) == isUniqueIndex(prevType)) ||
                   (suspect instanceof ColumnRef && !isUniqueIndexColumn(suspect))) {
            return null;
        }
        return "Incompatible schema between master and replica: field " + field + " in schema object " + suspect;
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
