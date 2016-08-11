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

package org.voltdb.settings;

import java.io.File;
import java.util.Map;

import org.aeonbits.owner.Config.Sources;
import org.aeonbits.owner.ConfigFactory;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;

@Sources("file:${org.voltdb.config.dir}/cluster.properties")
public interface ClusterSettings extends Settings {
    public final static String CONFIG_DIR = "org.voltdb.config.dir";

    // property keys
    public final static String HOST_COUNT = "org.voltdb.cluster.hostcount";
    public final static String CANGAMANGA = "dumy.cangamanga";

    @Key(HOST_COUNT)
    @DefaultValue("1")
    public int hostcount();

    @Key(CANGAMANGA)
    @DefaultValue("7")
    public int cangamanga();

    public static ClusterSettings create(Map<?, ?>... imports) {
        return ConfigFactory.create(ClusterSettings.class, imports);
    }

    public static ClusterSettings create(byte [] bytes) {
        return create(Settings.bytesToProperties(bytes));
    }

    default void store() {
        final String configDN = ConfigFactory.getProperty(CONFIG_DIR).intern();
        if (configDN == null || configDN.trim().isEmpty()) {
            throw new IllegalStateException("property " + CONFIG_DIR + " must be defined");
        }
        File configDH = new File(configDN);
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
        store(new File(configDH, "cluster.properties"), "VoltDB cluster settings");
    }

    default Supplier<ClusterSettings> asSupplier() {
        return Suppliers.ofInstance(this);
    }

}
