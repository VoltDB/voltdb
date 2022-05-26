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

package org.voltdb;

/**
 * Give the enterprise code a chance to do any enterprise-feature specific
 * periodic maintenance.
 *
 */
public abstract class EnterpriseMaintenance {

    private static boolean m_searched = false;
    private static EnterpriseMaintenance m_globalEM = null;

    static synchronized EnterpriseMaintenance get() {
        // use the cached version
        if (m_searched) {
            return m_globalEM;
        }

        // try to find the pro class and leave the global as null if no luck
        Class<?> clz = null;
        try {
            clz = Class.forName("org.voltdb.EnterpriseMaintenanceImpl");
        }
        catch (ClassNotFoundException e) {
            m_searched = true;
            return null;
        }

        // try to instantiate the pro class and leave as null if no luck
        try {
            m_globalEM = (EnterpriseMaintenance) clz.newInstance();
        }
        catch (Exception e) {}

        m_searched = true;
        return m_globalEM;
    }

    abstract void setupMaintenanceTasks();
    abstract void dailyMaintenanceTask();
}
