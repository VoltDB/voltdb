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

import java.util.concurrent.atomic.AtomicStampedReference;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltdb.VoltZK;

import com.google_voltpatches.common.base.Supplier;

public class ClusterSettingsRef extends AtomicStampedReference<ClusterSettings>
        implements Supplier<ClusterSettings> {
    static final public int UNVERSIONED = -1;

    public ClusterSettingsRef() {
        super(null, UNVERSIONED);
    }

    public int store(ZooKeeper zk) {
        Stat stat = null;
        int [] stamp = new int[]{0};
        ClusterSettings settings = get(stamp);
        try {
            // at this stage zookeeper is one version behind
            stat = zk.setData(VoltZK.cluster_settings, settings.asBytes(), stamp[0]-1);
        } catch (KeeperException|InterruptedException e) {
            throw new SettingsException("Failed to store to ZooKeeper", e);
        }
        return stat.getVersion();
    }

    public int load(ZooKeeper zk) {
        byte [] bytes = null;
        Stat stat = new Stat();
        try {
            bytes = zk.getData(VoltZK.cluster_settings, false, stat);
        } catch (KeeperException|InterruptedException e) {
            throw new SettingsException("Failed to initialize from ZooKeeper", e);
        }

        if (bytes==null) {
            throw new SettingsException("Failed to initialize cluster settings from ZooKeeper");
        }
        set(ClusterSettings.create(bytes), stat.getVersion());
        return stat.getVersion();
    }

    @Override
    public ClusterSettings get() {
        return getReference();
    }

    public ClusterSettingsRef(ClusterSettings initialValue) {
        super(initialValue,1);
    }
}
