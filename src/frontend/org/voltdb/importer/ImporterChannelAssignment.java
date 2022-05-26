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
package org.voltdb.importer;

import java.net.URI;
import java.util.Set;

public class ImporterChannelAssignment {

    final int version;
    final String importer;
    final Set<URI> added;
    final Set<URI> removed;
    final Set<URI> assigned;

    ImporterChannelAssignment(
            String importer,
            Set<URI> added,
            Set<URI> removed,
            Set<URI> assigned,
            int version
    ) {
        if (importer == null || importer.trim().isEmpty()) {
            throw new IllegalArgumentException("null or empty or blank importer");
        }
        if (version < 0) {
            throw new IllegalArgumentException("version is less than 0");
        }
        if (added == null) {
            throw new IllegalArgumentException("added is null");
        }
        if (removed == null) {
            throw new IllegalArgumentException("removed is null");
        }
        if (assigned == null) {
            throw new IllegalArgumentException("assigned is null");
        }

        this.version = version;
        this.importer = importer;
        this.added = added;
        this.removed = removed;
        this.assigned = assigned;
    }

    public int getVersion() {
        return version;
    }

    public String getImporter() {
        return importer;
    }

    public Set<URI> getAdded() {
        return added;
    }

    public Set<URI> getRemoved() {
        return removed;
    }

    public Set<URI> getAssigned() {
        return assigned;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((added == null) ? 0 : added.hashCode());
        result = prime * result
                + ((assigned == null) ? 0 : assigned.hashCode());
        result = prime * result
                + ((importer == null) ? 0 : importer.hashCode());
        result = prime * result + ((removed == null) ? 0 : removed.hashCode());
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ImporterChannelAssignment other = (ImporterChannelAssignment) obj;
        if (added == null) {
            if (other.added != null)
                return false;
        } else if (!added.equals(other.added))
            return false;
        if (assigned == null) {
            if (other.assigned != null)
                return false;
        } else if (!assigned.equals(other.assigned))
            return false;
        if (importer == null) {
            if (other.importer != null)
                return false;
        } else if (!importer.equals(other.importer))
            return false;
        if (removed == null) {
            if (other.removed != null)
                return false;
        } else if (!removed.equals(other.removed))
            return false;
        if (version != other.version)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ImporterChannelAssignment [importer=" + importer + ", added="
                + added + ", removed=" + removed + ", assigned=" + assigned
                + ", version=" + version + "]";
    }
}
