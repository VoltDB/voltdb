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

package org.voltdb.compiler;

import org.voltdb.client.ProcedureInvocationType;

public class CatalogChangeResult {

    public String errorMsg = null;
    public int diffCommandsLength;
    public String encodedDiffCommands;
    public byte[] catalogBytes;
    public byte[] catalogHash;
    public byte[] deploymentBytes;
    public byte[] deploymentHash;
    public String[] tablesThatMustBeEmpty;
    public String[] reasonsForEmptyTables;
    public boolean requiresSnapshotIsolation;
    public boolean worksWithElastic;
    public ProcedureInvocationType invocationType;
    // null or source version string if an automatic upgrade was done.
    public String upgradedFromVersion;

    public boolean isForReplay;
    // Should catalog diff commands apply to EE or not
    public boolean requireCatalogDiffCmdsApplyToEE;
    // mark it false for UpdateClasses, in future may be marked false for deployment changes
    public boolean hasSchemaChange;
    public int expectedCatalogVersion = -1;
    // In CL replay the catalog version may not strictly increase by 1, because failed UAC also consumes a version number.
    public int nextCatalogVersion = -1;
    // This is set to true if schema change involves stream or connector changes or a view on stream is created or dropped.
    public boolean requiresNewExportGeneration;
    // This is true if there are security user changes.
    public boolean hasSecurityUserChange;
    // True if online change not supported (specifically for @UpdateApplicationCatalog use)
    public boolean dynamicChangeNotSupported;
}
