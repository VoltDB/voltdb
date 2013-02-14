/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.utils;

public class EstTimeUpdater {
    public static boolean update(final long now) {
        final long estNow = EstTime.m_now.get();
        if (estNow == now) {
            return false;
        }
        EstTime.m_now.lazySet(now);
        /*
         * Check if updating the estimated time was especially tardy.
         * I am concerned that the thread responsible for updating the estimated
         * time might be blocking on something and want to be able to log if
         * that happens
         */
        if (now - estNow > 2000) {
            return true;
        } else {
            return false;
        }
    }
}
