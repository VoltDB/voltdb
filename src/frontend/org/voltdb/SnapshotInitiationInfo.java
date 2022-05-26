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

package org.voltdb;

import java.net.URI;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;

/**
 * Encapsulate the parameters provided to @SnapshotSave needed to initiate a snapshot.
 * Handle parameter parsing, error generation, etc.
 */
public class SnapshotInitiationInfo
{
    private String m_path;
    private String m_nonce;
    private boolean m_blocking;
    private SnapshotFormat m_format;
    private String m_data;
    private boolean m_truncationRequest;
    private SnapshotPathType m_stype;
    private Long m_terminus = null;
    public static final String MAGIC_NONCE_PREFIX = "MANUAL";

    /**
     * Construct the object given the parameters directly.
     * @param data any additional JSON blob params.  Currently only provided by VoltDB internals
     */
    public SnapshotInitiationInfo(String path, String nonce, boolean blocking,
            SnapshotFormat format, SnapshotPathType stype, String data)
    {
        m_path = path;
        m_nonce = nonce;
        m_blocking = blocking;
        m_format = format;
        m_data = data;
        m_truncationRequest = false;
        m_stype = stype;
        m_terminus = null;
        if (data != null && !data.trim().isEmpty()) {
            try {
                JSONObject jo = new JSONObject(data);
                if (jo.has(SnapshotUtil.JSON_TERMINUS)) {
                    m_terminus = jo.getLong(SnapshotUtil.JSON_TERMINUS);
                }
            } catch (JSONException failedToFishForTerminus) {
            }
        }
    }

    /**
     * Construct the object based on the params from the stored procedure invocation.
     */
    public SnapshotInitiationInfo(Object[] params) throws Exception
    {
        m_path = null;
        m_nonce = null;
        m_blocking = true;
        m_format = SnapshotFormat.NATIVE;
        m_data = null;
        m_truncationRequest = false;
        boolean checkNonceValidity = true;
        switch (params.length) {
            case 3:
                parseLegacyParams(params);
                break;
            case 1:
                checkNonceValidity = parseJsonParams(params);
                break;
            case 0:
                m_nonce = MAGIC_NONCE_PREFIX + System.currentTimeMillis();
                m_stype = SnapshotPathType.SNAP_AUTO;
                m_path = VoltDB.instance().getSnapshotPath();
                m_blocking = false;
                //We will always generate a good valid nonce.
                checkNonceValidity = false;
                break;
            default:
                throw new IllegalArgumentException("Invalid number of parameters, " + params.length + ". @SnapshotSave allows, 0, 1, or 3 parameters.");
        }

        if (checkNonceValidity && m_nonce != null && (m_nonce.contains("-") || m_nonce.contains(",") || m_nonce.startsWith(MAGIC_NONCE_PREFIX))) {
            throw new IllegalArgumentException("Provided nonce " + m_nonce + " contains a prohibited character (- or ,) or starts with " + MAGIC_NONCE_PREFIX);
        }
    }

    private void parseLegacyParams(Object[] params) throws Exception {
        if (params[0] == null) {
            throw new Exception("@SnapshotSave path is null");
        }
        if (params[1] == null) {
            throw new Exception("@SnapshotSave nonce is null");
        }
        if (params[2] == null) {
            throw new Exception("@SnapshotSave blocking is null");
        }
        if (!(params[0] instanceof String)) {
            throw new Exception("@SnapshotSave path param is a " +
                    params[0].getClass().getSimpleName() +
                    " and should be a java.lang.String");
        }
        if (!(params[1] instanceof String)) {
            throw new Exception("@SnapshotSave nonce param is a " +
                    params[0].getClass().getSimpleName() +
                    " and should be a java.lang.String");
        }
        if (!(params[2] instanceof Byte ||
                    params[2] instanceof Short ||
                    params[2] instanceof Integer ||
                    params[2] instanceof Long))
        {
            throw new Exception("@SnapshotSave blocking param is a " +
                    params[0].getClass().getSimpleName() +
                    " and should be a java.lang.[Byte|Short|Integer|Long]");
        }

        m_path = (String)params[0];
        m_nonce = (String)params[1];
        m_blocking = ((Number)params[2]).byteValue() == 0 ? false : true;
        m_format = SnapshotFormat.NATIVE;
        m_stype = SnapshotPathType.SNAP_PATH;
    }

    /**
     * Parse the JSON blob.  Schema is roughly:
     * keys:
     *   (optional) service: currently only 'log_truncation' is valid.  Will
     *   induce a command log truncation snapshot if command logging is present
     *   and enabled.  This will cause all other keys to be unnecessary and/or
     *   ignored.
     *
     *   uripath: the URI where the snapshot should be written to.  Only file:// URIs are currently accepted.
     *
     *   nonce: the nonce to be used for the snapshot.  '-' and ',' are forbidden nonce characters
     *
     *   block: whether or not the snapshot should block the database or not
     *   while it's being generated.  All non-zero numbers will be interpreted
     *   as blocking.  true/false will be interpreted as you'd expect
     *
     *   format: one of 'native' or 'csv'.
     */
    private boolean parseJsonParams(Object[] params) throws Exception
    {
        boolean checkValidity = true;
        if (params[0] == null) {
            throw new Exception("@SnapshotSave JSON blob is null");
        }
        if (!(params[0] instanceof String)) {
            throw new Exception("@SnapshotSave JSON blob is a " +
                    params[0].getClass().getSimpleName() +
                    " and should be a java.lang.String");
        }
        final JSONObject jsObj = new JSONObject((String)params[0]);

        // IZZY - Make service an enum and store it in the object if
        // we every introduce another one
        if (jsObj.has(SnapshotUtil.JSON_SERVICE)) {
            String service = jsObj.getString(SnapshotUtil.JSON_SERVICE);
            if (service.equalsIgnoreCase("log_truncation")) {
                m_truncationRequest = true;
                if (!VoltDB.instance().getCommandLog().isEnabled()) {
                    throw new Exception("Cannot ask for a command log truncation snapshot when " +
                            "command logging is not present or enabled.");
                }
                // for CL truncation, don't care about any of the rest of the blob.
                return checkValidity;
            }
            else {
                throw new Exception("Unknown snapshot save service type: " + service);
            }
        }

        m_stype = SnapshotPathType.valueOf(jsObj.optString(SnapshotUtil.JSON_PATH_TYPE, SnapshotPathType.SNAP_PATH.toString()));
        if (jsObj.has(SnapshotUtil.JSON_URIPATH)) {
            m_path = jsObj.getString(SnapshotUtil.JSON_URIPATH);
            if (m_path.isEmpty()) {
                throw new Exception("uripath cannot be empty");
            }
        } else {
            m_stype = SnapshotPathType.SNAP_AUTO;
            m_path = "file:///" + VoltDB.instance().getCommandLogSnapshotPath();
        }
        URI pathURI = new URI(m_path);
        String pathURIScheme = pathURI.getScheme();
        if (pathURIScheme == null) {
            throw new Exception("URI scheme cannot be null");
        }
        if (!pathURIScheme.equals("file")) {
            throw new Exception("Unsupported URI scheme " + pathURIScheme +
                    " if this is a file path then you must prepend file://");
        }
        m_path = pathURI.getPath();
        if (jsObj.has(SnapshotUtil.JSON_NONCE)) {
            m_nonce = jsObj.getString(SnapshotUtil.JSON_NONCE);
            if (m_nonce.isEmpty()) {
                throw new Exception("nonce cannot be empty");
            }
        } else {
            m_nonce = MAGIC_NONCE_PREFIX + System.currentTimeMillis();
            //This is a valid JSON
            checkValidity = false;
        }

        Object blockingObj = false;
        if (jsObj.has(SnapshotUtil.JSON_BLOCK)) {
            blockingObj = jsObj.get(SnapshotUtil.JSON_BLOCK);
        }
        if (blockingObj instanceof Number) {
            m_blocking = ((Number)blockingObj).byteValue() == 0 ? false : true;
        } else if (blockingObj instanceof Boolean) {
            m_blocking = (Boolean)blockingObj;
        } else if (blockingObj instanceof String) {
            m_blocking = Boolean.valueOf((String)blockingObj);
        } else {
            throw new Exception(blockingObj.getClass().getName() + " is not supported as " +
                    " type for the block parameter");
        }

        m_format = SnapshotFormat.NATIVE;
        String formatString = jsObj.optString(SnapshotUtil.JSON_FORMAT,SnapshotFormat.NATIVE.toString());

        m_terminus = jsObj.has(SnapshotUtil.JSON_TERMINUS) ? jsObj.getLong(SnapshotUtil.JSON_TERMINUS) : null;
        /*
         * Try and be very flexible about what we will accept
         * as the type of the block parameter.
         */
        try {
            m_format = SnapshotFormat.getEnumIgnoreCase(formatString);
        } catch (IllegalArgumentException argException) {
            throw new Exception("@SnapshotSave format param is a " + m_format +
                    " and should be one of [\"native\" | \"csv\"]");
        }
        m_data = (String)params[0];
        return checkValidity;
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

    public boolean isTruncationRequest()
    {
        return m_truncationRequest;
    }

    public String getJSONBlob()
    {
        return m_data;
    }

    public Long getTerminus()
    {
        return m_terminus;
    }

    /**
     * When we write to ZK to request the snapshot, generate the JSON which will be written to the node's data.
     */
    public JSONObject getJSONObjectForZK() throws JSONException
    {
        final JSONObject jsObj = new JSONObject();
        jsObj.put(SnapshotUtil.JSON_PATH, m_path);
        jsObj.put(SnapshotUtil.JSON_PATH_TYPE, m_stype.toString());
        jsObj.put(SnapshotUtil.JSON_NONCE, m_nonce);
        jsObj.put(SnapshotUtil.JSON_BLOCK, m_blocking);
        jsObj.put(SnapshotUtil.JSON_FORMAT, m_format.toString());
        jsObj.putOpt(SnapshotUtil.JSON_DATA, m_data);
        jsObj.putOpt(SnapshotUtil.JSON_TERMINUS, m_terminus);
        return jsObj;
    }
}
