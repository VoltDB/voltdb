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

package org.voltdb.iv2;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.base.Charsets;

public class MigratePartitionLeaderInfo {

    private long m_oldLeaderHsid = Long.MIN_VALUE;
    private long m_newLeaderHsid = Long.MIN_VALUE;
    private int m_partitionId = Integer.MIN_VALUE;

    public MigratePartitionLeaderInfo() { }

    public MigratePartitionLeaderInfo(byte[] data) throws JSONException {
        JSONObject jsObj = new JSONObject(new String(data, Charsets.UTF_8));
        m_oldLeaderHsid = jsObj.getLong("oldHsid");
        m_newLeaderHsid = jsObj.getLong("newHsid");
        m_partitionId = jsObj.getInt("partitionId");
    }

    public MigratePartitionLeaderInfo(long oldHsid, long newHsid, int partitionId) {
        m_oldLeaderHsid = oldHsid;
        m_newLeaderHsid = newHsid;
        m_partitionId = partitionId;
    }

    public byte[] toBytes() throws JSONException {
        JSONStringer js = new JSONStringer();
        js.object();
        js.key("oldHsid").value(m_oldLeaderHsid);
        js.key("newHsid").value(m_newLeaderHsid);
        js.key("partitionId").value(m_partitionId);
        js.endObject();
        return js.toString().getBytes(Charsets.UTF_8);
    }

    public long getOldLeaderHsid() {
        return m_oldLeaderHsid;
    }

    public long getNewLeaderHsid() {
        return m_newLeaderHsid;
    }

    public int getOldLeaderHostId() {
        return CoreUtils.getHostIdFromHSId(m_oldLeaderHsid);
    }

    public int getNewLeaderHostId() {
        return CoreUtils.getHostIdFromHSId(m_newLeaderHsid);
    }

    public int getPartitionId() {
        return m_partitionId;
    }
}
