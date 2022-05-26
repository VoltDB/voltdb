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

package org.voltcore.messaging;

import java.nio.ByteBuffer;
import java.util.Set;

import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.collect.ImmutableSet;


public final class FaultMessage extends VoltMessage {

    public final long failedSite;
    public final boolean witnessed;
    public final boolean decided;
    public final long reportingSite;
    public final Set<Long> survivors;

    public FaultMessage(final long reportingSite, final long failedSite) {
        this.failedSite = failedSite;
        this.reportingSite = reportingSite;
        this.witnessed = true;
        this.survivors = ImmutableSet.of();
        this.decided = false;
    }

    public FaultMessage(final long reportingSite,
            final long failedSite, final Set<Long> survivors) {
        this.failedSite = failedSite;
        this.reportingSite = reportingSite;
        this.witnessed = false;
        this.survivors = ImmutableSet.copyOf(survivors);
        this.decided = false;
    }

    public FaultMessage(
            final long reportingSite,
            final long failedSite,
            final Set<Long> survivors,
            final boolean decision) {
        this.failedSite = failedSite;
        this.reportingSite = reportingSite;
        this.witnessed = false;
        this.survivors = ImmutableSet.copyOf(survivors);
        this.decided = decision;
    }

    @Override
    public int getSerializedSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getSubject() {
        return Subject.FAILURE.getId();
    }

    public boolean isSoleSurvivor() {
        return !witnessed
            && survivors.size() == 1
            && survivors.contains(reportingSite);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FaultMessage {failed: ");
        sb.append(CoreUtils.hsIdToString(failedSite));
        sb.append(", reporting: ");
        sb.append(CoreUtils.hsIdToString(reportingSite));
        if (!witnessed) {
            if (decided) {
                sb.append(", decided:").append(decided);
            }
            sb.append(", survivors: [");
            sb.append(CoreUtils.hsIdCollectionToString(survivors));
            sb.append("]");
        } else {
            sb.append(", witnessed: ").append(witnessed);
        }
        sb.append("}");
        return sb.toString();
    }
}
