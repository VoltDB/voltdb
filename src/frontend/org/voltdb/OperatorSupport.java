/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
 * This class serves to hook up VoltDB with code that is not
 * present in the community edition but which is needed to
 * facilitate operation under Kubernetes.
 *
 * This implementation provides dummy support; it will be
 * replaced by a functional implementation at initialization
 * where appropriate.
 */
public class OperatorSupport {
    public void registerStatistics(StatsAgent sa) { }
    public void startStatusListener(VoltDB.Configuration config) { }
    public void stopStatusListener() { }
}
