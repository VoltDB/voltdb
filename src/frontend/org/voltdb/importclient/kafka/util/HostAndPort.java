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

package org.voltdb.importclient.kafka.util;

public class HostAndPort {

    private final String m_host;
    private final int m_port;
    private final String m_connectionString;

    public HostAndPort(String h, int p) {
        m_host = h;
        m_port = p;
        m_connectionString = m_host + ":" + m_port;
    }

    public static HostAndPort fromString(String hap) {
        String s[] = hap.split(":");
        int p = KafkaConstants.KAFKA_DEFAULT_BROKER_PORT;
        if (s.length > 1 && s[1] != null && s[1].length() > 0) {
            p = Integer.parseInt(s[1].trim());
        }
        return new HostAndPort(s[0].trim(), p);
    }

    public String getHost() {
        return m_host;
    }

    public int getPort() {
        return m_port;
    }

    @Override
    public String toString() {
        return m_connectionString;
    }

    @Override
    public int hashCode() {
        return m_connectionString.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof HostAndPort)) {
            return false;
        }
        if (this.getClass() != o.getClass()) {
            return false;
        }
        HostAndPort hap = (HostAndPort )o;
        if (hap == this) {
            return true;
        }
        return (hap.getHost().equals(getHost()) && hap.getPort() == getPort());
    }
}
