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
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.ImmutableSetMultimap;
import com.google_voltpatches.common.collect.SetMultimap;
import com.google_voltpatches.common.collect.Sets;

public class ChannelAssignment {

    final Set<ChannelSpec> added;
    final Set<ChannelSpec> removed;
    final NavigableSet<ChannelSpec> channels;
    final int version;
    final List<ImporterChannelAssignment> assignments;

    ChannelAssignment(NavigableSet<ChannelSpec> prev, NavigableSet<ChannelSpec> next, int version) {
        this.version  = version;
        this.added    = Sets.difference(next, prev);
        this.removed  = Sets.difference(prev, next);
        this.channels = next;

        this.assignments = perImporterAssignments();
    }

    public Set<ChannelSpec> getAdded() {
        return added;
    }

    public Set<ChannelSpec> getRemoved() {
        return removed;
    }

    public NavigableSet<ChannelSpec> getChannels() {
        return channels;
    }

    public int getVersion() {
        return version;
    }

    public boolean hasChanges() {
        return !removed.isEmpty() || !added.isEmpty();
    }

    @Override
    public String toString() {
        return "ChannelAssignment [added=" + added + ", removed=" + removed
                + ", channels=" + channels + ", version=" + version + "]";
    }

    public List<ImporterChannelAssignment> getImporterChannelAssignments() {
        return assignments;
    }

    private SetMultimap<String, URI> mapByImporter(Set<ChannelSpec> specs) {
        ImmutableSetMultimap.Builder<String, URI> mmbldr = ImmutableSetMultimap.builder();
        for (ChannelSpec spec: specs) {
            mmbldr.put(spec.getImporter(),spec.getUri());
        }
        return mmbldr.build();
    }

    private List<ImporterChannelAssignment> perImporterAssignments() {

        ImmutableSet.Builder<String> sbldr = ImmutableSet.builder();
        for (ChannelSpec spec: Sets.union(added, removed)) {
            sbldr.add(spec.getImporter());
        }

        ImmutableList.Builder<ImporterChannelAssignment> lbldr = ImmutableList.builder();

        final SetMultimap<String, URI> added = mapByImporter(getAdded());
        final SetMultimap<String, URI> removed = mapByImporter(getRemoved());
        final SetMultimap<String, URI> assigned = mapByImporter(getChannels());

        for (String importer: sbldr.build()) {
            lbldr.add(new ImporterChannelAssignment(
                    importer,
                    added.get(importer),
                    removed.get(importer),
                    assigned.get(importer),
                    version
                    ));
        }
        return lbldr.build();
    }
}
