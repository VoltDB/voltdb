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

package org.voltdb;

import java.net.URI;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

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

    /**
     * Construct the object given the parameters directly.
     * @param data any additional JSON blob params.  Currently only provided by VoltDB internals
     */
    public SnapshotInitiationInfo(String path, String nonce, boolean blocking,
            SnapshotFormat format, String data)
    {
        m_path = path;
        m_nonce = nonce;
        m_blocking = blocking;
        m_format = format;
        m_data = data;
        m_truncationRequest = false;
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

        if (params.length == 3) {
            parseLegacyParams(params);
        }
        else if (params.length == 1) {
            parseJsonParams(params);
        }
        else {
            throw new Exception("@SnapshotSave requires 3 parameters " +
                    "(Path, nonce, and blocking) or alternatively a single JSON blob. ");
        }

        if (m_nonce != null && (m_nonce.contains("-") || m_nonce.contains(","))) {
            throw new Exception("Provided nonce " + m_nonce + " contains a prohibited character (- or ,)");
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
    private void parseJsonParams(Object[] params) throws Exception
    {
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
        if (jsObj.has("service")) {
            String service = jsObj.getString("service");
            if (service.equalsIgnoreCase("log_truncation")) {
                m_truncationRequest = true;
                if (!VoltDB.instance().getCommandLog().isEnabled()) {
                    throw new Exception("Cannot ask for a command log truncation snapshot when " +
                            "command logging is not present or enabled.");
                }
                // for CL truncation, don't care about any of the rest of the blob.
                return;
            }
            else {
                throw new Exception("Unknown snapshot save service type: " + service);
            }
        }

        m_path = jsObj.getString("uripath");
        if (m_path.isEmpty()) {
            throw new Exception("uripath cannot be empty");
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

        m_nonce = jsObj.getString("nonce");
        if (m_nonce.isEmpty()) {
            throw new Exception("nonce cannot be empty");
        }

        Object blockingObj = false;
        if (jsObj.has("block")) {
            blockingObj = jsObj.get("block");
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
        String formatString = jsObj.optString("format",SnapshotFormat.NATIVE.toString());
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

    /**
     * When we write to ZK to request the snapshot, generate the JSON which will be written to the node's data.
     */
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
}
