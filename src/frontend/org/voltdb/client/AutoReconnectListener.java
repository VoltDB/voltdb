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
package org.voltdb.client;

import org.voltdb.utils.CSVDataLoader;

public class AutoReconnectListener extends ClientStatusListenerExt {

    private CSVDataLoader m_dataLoader;

    @Override
    public void connectionCreated(String hostname, int port, AutoConnectionStatus status) {
        if (m_dataLoader != null && status == AutoConnectionStatus.SUCCESS) {
            m_dataLoader.resumeLoading();
        }
    }

    public void setLoader(CSVDataLoader loader) {
        m_dataLoader = loader;
    }

}
