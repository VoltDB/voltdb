/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
     */
    public void newSegmentAdded();

    /**
     * Callback from BinaryDeque saying bytes were added to the PBD and how many bytes.
     * @param numBytes
     */
    public void bytesAdded(long numBytes);

    /**
     * The id of the cursor that this retention policy implementation uses to read data from the {@link BinaryDeque}.
     *
     * @return cursor id used to read data from the {@link BinaryDeque}
     */
    public String getCursorId();
}
