/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.importclient.socket;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.importer.ImporterConfig;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 */
public class PullSocketImporterConfig implements ImporterConfig {

    private final static Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
    private final static Pattern HOST_RE = Pattern.compile(
                "(?<host>[\\w._-]+):(?<port>\\d+)(?:-(?<tail>\\d+)){0,1}"
            );

    private Map<URI,String> m_resources = ImmutableMap.of();

    @Override
    public void addConfiguration(Properties props)
    {
        String hosts = props.getProperty("addresses", "").trim();
        if (hosts.isEmpty()) {
            throw new IllegalArgumentException("'adresses' is a required property and must be defined");
        }
        String procedure = props.getProperty("procedure", "").trim();
        if (procedure.isEmpty()) {
            throw new IllegalArgumentException("'procedure' is a required property and must be defined");
        }

        ImmutableMap.Builder<URI,String> sbldr = ImmutableMap.builder();
        sbldr.putAll(m_resources);
        for (String host: COMMA_SPLITTER.split(hosts)) {
            sbldr.putAll(checkHost(host, procedure));
        }
        try {
            m_resources = sbldr.build();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "One or more addresses are assigned to more than one store procedure", e);
        }
    }

    private final Map<URI,String> checkHost(String hspec, String procedure) {
        Matcher mtc = HOST_RE.matcher(hspec);
        if (!mtc.matches()) {
            throw new IllegalArgumentException(String.format("Address spec %s is malformed", hspec));
        }
        int port = Integer.parseInt(mtc.group("port"));
        int tail = port;
        if (mtc.group("tail") != null) {
            tail = Integer.parseInt(mtc.group("tail"));
        }
        if (port>tail) {
            throw new IllegalArgumentException(String.format("Invalid port range in address spec %s", hspec));
        }
        ImmutableMap.Builder<URI,String> mbldr = ImmutableMap.builder();
        for (int p = port; p <= tail; ++p) {
            InetAddress a;
            try {
                a = InetAddress.getByName(mtc.group("host"));
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(String.format("Failed to resolve host %s", mtc.group("host")), e);
            }
            InetSocketAddress sa = new InetSocketAddress(a, p);
            mbldr.put(URI.create("tcp://" + sa.getHostString() + ":" + sa.getPort() + "/"),procedure);
        }
        return mbldr.build();
    }

    @Override
    public Set<URI> getAvailableResources()
    {
        return m_resources.keySet();
    }

    String getProcedure(URI uri) {
        return m_resources.get(uri);
    }
}
