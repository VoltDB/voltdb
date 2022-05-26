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

package org.voltdb.utils;

import java.io.IOException;

/**
 * Interface to be implemented by a retention policy on a {@link BinaryDeque}. This has methods required to start and stop
 * retention policy enforcement as well as for the callbacks from the {@link BinaryDeque} to the policy implementation.
 */
interface PBDRetentionPolicy {
    /**
     * Start running retention policy enforcement.
     *
     * @throws IOException if an error occurs trying to read {@link BinaryDeque} data
     */
    public void startPolicyEnforcement() throws IOException;

    /**
     * Stop enforcing retention policy and release any resources.
     */
    public void stopPolicyEnforcement();

    /**
     * @return true if the policy is being enforced
     */
    public boolean isPolicyEnforced();

    /**
     * This will be used by the {@link BinaryDeque} to notify the retention policy implementation that a new segment of data
     * has been added to it.
     * @param The length of the file at initialization time. This is usually the size of the segment header.
     */
    public void newSegmentAdded(long initialBytes);

    /**
     * Callback from BinaryDeque saying bytes were added to the PBD and how many bytes.
     * @param numBytes
     */
    public void bytesAdded(long numBytes);

    /**
     * Gaps may be filled anywhere in the PBD, so retention policy enforcement may
     * need to do extra actions once gaps are filled. This callback is used to
     * notify retention policy enforcement that a gap segment processing was completed.
     */
    public void finishedGapSegment();

    /**
     * The id of the cursor that this retention policy implementation uses to read data from the {@link BinaryDeque}.
     *
     * @return cursor id used to read data from the {@link BinaryDeque}
     */
    public String getCursorId();
}
