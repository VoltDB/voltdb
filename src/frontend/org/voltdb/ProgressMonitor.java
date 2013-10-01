/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
package org.voltdb;

/**
 * System Procedure progress callback interface. Start indicates the total result size expected. Progress updates the
 * work done so far. End indicates that processing is done. Implementer needs to handle these and track the counters and
 * report progress to user or some other system.
 */
public interface ProgressMonitor {

    void reportProgress(VoltTable table);

    void reportStart(VoltTable table);

    void reportEnd(VoltTable table);
}
