/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.importer.ImporterConfig;
import org.voltdb.importer.formatter.AbstractFormatterFactory;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * ImporterConfig implementation for pull socket importer. There will be an ImporterConfig per
 * resource ID.
 */
public class PullSocketImporterConfig implements ImporterConfig
{
    private final static Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
    private final static Pattern HOST_RE = Pattern.compile(
                "(?<host>[\\w._-]+):(?<port>\\d+)(?:-(?<tail>\\d+)){0,1}"
            );


    private final URI m_resourceID;
    private final String m_procedure;
    private final AbstractFormatterFactory m_formatterFactory;
    private final Properties m_formatProps;
    private final String m_formatName;
    public PullSocketImporterConfig(URI resourceID, String procedure, AbstractFormatterFactory formatterFactory, String formatName, Properties formatProps)
    {
        m_resourceID = resourceID;
        m_procedure = procedure;
        m_formatterFactory = formatterFactory;
        m_formatName = formatName;
        m_formatProps = formatProps;
    }

    @Override
    public URI getResourceID()
    {
        return m_resourceID;
    }

    @Override
    public AbstractFormatterFactory getFormatterFactory()
    {
        return m_formatterFactory;
    }

    String getProcedure() {
        return m_procedure;
    }

    public static Map<URI, ImporterConfig> createConfigEntries(Properties props, String formatName,  Properties formatProps,  AbstractFormatterFactory formatterFactory)
    {
        String hosts = props.getProperty("addresses", "").trim();
        if (hosts.isEmpty()) {
            throw new IllegalArgumentException("'adresses' is a required property and must be defined");
        }
        String procedure = props.getProperty("procedure", "").trim();
        if (procedure.isEmpty()) {
            throw new IllegalArgumentException("'procedure' is a required property and must be defined");
        }

        ImmutableMap.Builder<URI, ImporterConfig> sbldr = ImmutableMap.builder();
        for (String host: COMMA_SPLITTER.split(hosts)) {
            checkHostAndAddConfig(host, procedure, sbldr, formatterFactory, formatName, formatProps);
        }
        try {
            return sbldr.build();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "One or more addresses are assigned to more than one store procedure", e);
        }
    }

    private static void checkHostAndAddConfig(String hspec, String procedure, ImmutableMap.Builder<URI, ImporterConfig> builder,
            AbstractFormatterFactory formatterFactory, String formatName,  Properties formatProps) {
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

        for (int p = port; p <= tail; ++p) {
            InetAddress a;
            try {
                a = InetAddress.getByName(mtc.group("host"));
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(String.format("Failed to resolve host %s", mtc.group("host")), e);
            }
            InetSocketAddress sa = new InetSocketAddress(a, p);
            PullSocketImporterConfig config =
                    new PullSocketImporterConfig(URI.create("tcp://" + sa.getHostString() + ":" + sa.getPort() + "/"), procedure,
                            formatterFactory, formatName, formatProps);
            builder.put(config.getResourceID(), config);
        }
    }

    @Override
    public Properties getFormatProps() {
        return m_formatProps;
    }

    @Override
    public String getFormatName() {
        return m_formatName;
    }
}
