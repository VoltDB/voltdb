/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package xdcrSelfCheck.resolves;

import org.json_voltpatches.JSONObject;

import java.util.Arrays;

public class XdcrConflict {

    public enum ROW_TYPE { EXT, EXP, NEW, DEL }

    public enum ACTION_TYPE { I, U, D }

    public enum CONFLICT_TYPE {
        MISS, MSMT, CNST, NONE,
        IICV, UUCV, IUCV, DDMR, UUTM, UDTM // no XDCR conflict and test only
    }

    public enum DECISION { A, R }

    public enum DIVERGENCE { C, D }

    private byte cid;
    private long rid;
    private long extRid;
    private String rowType;
    private String actionType;
    private String conflictType;
    private int conflictOnPrimaryKey;
    private String decision;
    private int clusterId;
    private String timeStamp;
    private String divergenceType;
    private String tableName;
    private int currentClusterId;
    private String currentTimestamp;
    private JSONObject tuple;

    private byte[] key;
    private byte[] value;

    public byte getCid() {
        return cid;
    }

    public void setCid(byte cid) {
        this.cid = cid;
    }

    public long getRid() {
        return rid;
    }

    public void setRid(long rid) {
        this.rid = rid;
    }

    public long getExtRid() {
        return extRid;
    }

    public void setExtRid(long extRid) {
        this.extRid = extRid;
    }

    public String getRowType() {
        return rowType;
    }

    public void setRowType(String rowType) {
        this.rowType = rowType;
    }

    public ROW_TYPE getRowTypeEnum() {
        return ROW_TYPE.valueOf(rowType);
    }

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public ACTION_TYPE getActionTypeEnum() {
        return ACTION_TYPE.valueOf(actionType);
    }

    public String getConflictType() {
        return conflictType;
    }

    public void setConflictType(String conflictType) {
        this.conflictType = conflictType;
    }

    public CONFLICT_TYPE getConflictTypeEnum() {
        return CONFLICT_TYPE.valueOf(conflictType);
    }

    public int getConflictOnPrimaryKey() {
        return conflictOnPrimaryKey;
    }

    public void setConflictOnPrimaryKey(int conflictOnPrimaryKey) {
        this.conflictOnPrimaryKey = conflictOnPrimaryKey;
    }

    public String getDecision() {
        return decision;
    }

    public void setDecision(String decision) {
        this.decision = decision;
    }

    public DECISION getDecisionEnum() {
        return DECISION.valueOf(decision);
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getDivergenceType() {
        return divergenceType;
    }

    public void setDivergenceType(String divergenceType) {
        this.divergenceType = divergenceType;
    }

    public DIVERGENCE getDivergenceTypeEnum() {
        return DIVERGENCE.valueOf(divergenceType);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getCurrentClusterId() {
        return currentClusterId;
    }

    public void setCurrentClusterId(int currentClusterId) {
        this.currentClusterId = currentClusterId;
    }

    public String getCurrentTimestamp() {
        return currentTimestamp;
    }

    public void setCurrentTimestamp(String currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
    }

    public JSONObject getTuple() {
        return tuple;
    }

    public void setTuple(JSONObject tuple) {
        this.tuple = tuple;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "XdcrConflict{" +
                "cid=" + cid +
                ", rid=" + rid +
                ", rowType='" + rowType + '\'' +
                ", actionType='" + actionType + '\'' +
                ", conflictType='" + conflictType + '\'' +
                ", conflictOnPrimaryKey=" + conflictOnPrimaryKey +
                ", decision='" + decision + '\'' +
                ", clusterId=" + clusterId +
                ", timeStamp='" + timeStamp + '\'' +
                ", divergenceType='" + divergenceType + '\'' +
                ", tableName='" + tableName + '\'' +
                ", currentClusterId=" + currentClusterId +
                ", currentTimestamp='" + currentTimestamp + '\'' +
                ", tuple=" + tuple +
                ", key=" + Arrays.toString(key) +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}
