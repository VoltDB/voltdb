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

package org.voltdb.settings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.UUID;

import org.voltdb.common.Constants;
import org.voltdb.utils.Digester;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.ConfigFactory;

import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.collect.ImmutableSortedMap;

public interface Settings extends Accessible {
    public final static String CONFIG_DIR = "org.voltdb.config.dir";

    public static void initialize(File voltdbroot) {
        if (ConfigFactory.getProperty(Settings.CONFIG_DIR) == null) try {
            File confDH = new File(voltdbroot, Constants.CONFIG_DIR).getCanonicalFile();
            String pathAsURI = confDH.toURI().getRawPath();
            ConfigFactory.setProperty(Settings.CONFIG_DIR, pathAsURI);
        } catch (IOException e) {
            throw new SettingsException("failed to resolve the cluster settings directory", e);
        }
    }

    default NavigableMap<String, String> asMap() {
        ImmutableSortedMap.Builder<String, String> mb = ImmutableSortedMap.naturalOrder();
        for (String name: propertyNames()) {
            mb.put(name, getProperty(name));
        }
        return mb.build();
    }

    default UUID getDigest() {
        return Digester.md5AsUUID(Joiner.on('#').withKeyValueSeparator("=").join(asMap()));
    }

    default byte [] asBytes() {
        return propertiesToBytes(asProperties());
    }

    default void store(File settingsFH, String comment) {
        try (FileOutputStream fos = new FileOutputStream(settingsFH)){
            asProperties().store(fos, comment);
            fos.getFD().sync(); // fsync to ensure file is persisted
        } catch (IOException e) {
            throw new SettingsException("failed to write properties to " + settingsFH, e);
        }
    }

    default Properties asProperties() {
        Properties props = new Properties();
        fill(props);
        return props;
    }

    public static Properties bytesToProperties(byte [] bytes) {
        Properties props = new Properties();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            props.load(bais);
        } catch (IOException e) {
            throw new SettingsException("failed to read properties from a byte array",e);
        }
        return props;
    }

    public static byte [] propertiesToBytes(Properties props) {
        byte [] bytes = new byte[0];
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(1024)) {
            props.store(baos, "VoltDB settings");
            bytes = baos.toByteArray();
        } catch (IOException e) {
            throw new SettingsException("failed to convert properties to a byte array",e);
        }
        return bytes;
    }

    static File getConfigDir() {
        final String configDN = ConfigFactory.getProperty(CONFIG_DIR).intern();
        if (configDN == null || configDN.trim().isEmpty()) {
            throw new IllegalStateException("property " + CONFIG_DIR + " must be defined");
        }
        URI uri = URI.create("file:" + configDN);
        File configDH = Paths.get(uri).toFile();
        if (!configDH.exists() && !configDH.mkdirs()) {
            throw new SettingsException("failed to create " + configDN);
        }
        if (   !configDH.isDirectory()
            || !configDH.canRead()
            || !configDH.canWrite()
            || !configDH.canExecute())
        {
            throw new SettingsException("cannot access " + configDN);
        }
        return configDH;
    }
}
