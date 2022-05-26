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

import java.io.File;
import java.util.Map;

import org.aeonbits.owner.Config.Sources;
import org.aeonbits.owner.ConfigFactory;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;

@Sources("file:${org.voltdb.config.dir}/cluster.properties")
public interface ClusterSettings extends Settings {
    // property keys
    public final static String HOST_COUNT = "org.voltdb.cluster.hostcount";
    public final static String CANGAMANGA = "dumy.cangamanga";
    public final static String PARTITITON_IDS = "org.voltdb.host.partitions";

    @Key(HOST_COUNT)
    @DefaultValue("1")
    public int hostcount();

    @Key(CANGAMANGA)
    @DefaultValue("7")
    public int cangamanga();

    @Key(PARTITITON_IDS)
    @DefaultValue("")
    public String partitionIds();

    public static ClusterSettings create(Map<?, ?>... imports) {
        return ConfigFactory.create(ClusterSettings.class, imports);
    }

    public static ClusterSettings create(byte [] bytes) {
        return create(Settings.bytesToProperties(bytes));
    }

    default void store() {
        File configFH = new File(Settings.getConfigDir(), "cluster.properties");
        store(configFH, "VoltDB cluster settings. DO NOT MODIFY THIS FILE!");
    }

    default Supplier<ClusterSettings> asSupplier() {
        return Suppliers.ofInstance(this);
    }
}
