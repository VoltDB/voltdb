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

/**
 * Interface for importer lifecycle support.
 * @author jcrump
 */
public interface ImporterLifecycle {

    /**
     * Lifecycle method.
     * @return Implementations should return true if this importer should be running, or false if it should begin the shutdown sequence.
     */
    public boolean shouldRun();

    /**
     * Stop this importer. After a call to this method, shouldRun() should return false.
     */
    public void stop();

    /**
     * Whether or not to submit transactions to Volt. Used only in test scenarios. See KafkaTopicTest for an example.
     * @return
     */
    public boolean hasTransaction();
}
