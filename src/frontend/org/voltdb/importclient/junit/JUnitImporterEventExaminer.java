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

package org.voltdb.importclient.junit;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Created by bshaw on 5/31/17.
 */
public interface JUnitImporterEventExaminer {

    /** Callback for making decisions based on importer state changes.
     * The provided map contains all events for all importers, but may not have an entry for every importer.
     * Events are only produced, never removed - but the examiner may remove events if it wants to.
     * This method has full write access to the map, but must be fast since this call blocks the event producers.
     * @param eventTracker Map of importers to their events.
     */
    void examine(Map<URI, List<JUnitImporter.Event>> eventTracker);
}
