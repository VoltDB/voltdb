/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

public class SnapshotInitiationInfo
{
    public SnapshotInitiationInfo(String path, String nonce, boolean blocking,
            SnapshotFormat format, String data)
    {
        m_path = path;
        m_nonce = nonce;
        m_blocking = blocking;
        m_format = format;
        m_data = data;
    }

    public String getPath()
    {
        return m_path;
    }

    public String getNonce()
    {
        return m_nonce;
    }

    public boolean isBlocking()
    {
        return m_blocking;
    }

    public SnapshotFormat getFormat()
    {
        return m_format;
    }

    public String getJSONBlob()
    {
        return m_data;
    }

    public JSONObject getJSONObjectForZK() throws JSONException
    {
        final JSONObject jsObj = new JSONObject();
        jsObj.put("path", m_path);
        jsObj.put("nonce", m_nonce);
        jsObj.put("block", m_blocking);
        jsObj.put("format", m_format.toString());
        jsObj.putOpt("data", m_data);
        return jsObj;
    }

    private String m_path;
    private String m_nonce;
    private boolean m_blocking;
    private SnapshotFormat m_format;
    private String m_data;
}
