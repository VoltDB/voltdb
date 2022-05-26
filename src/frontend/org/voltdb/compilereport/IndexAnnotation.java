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

package org.voltdb.compilereport;

import java.util.SortedSet;
import java.util.TreeSet;

import org.voltdb.catalog.Procedure;

/**
 * Extra information generated during the compilation process, used by the ReportMaker.
 * Gets attached to the m_annotation field in CatalogType.
 * This one is for indexes.
 *
 */
public class IndexAnnotation {
    // Procedures are inserted when the query statements inside use the index
    public SortedSet<Procedure> proceduresThatUseThis = new TreeSet<Procedure>();
}
