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

import org.json_voltpatches.JSONException;
import xdcrSelfCheck.resolves.XdcrConflict.ACTION_TYPE;
import xdcrSelfCheck.resolves.XdcrConflict.CONFLICT_TYPE;

import java.util.List;

import static xdcrSelfCheck.resolves.ConflictResolveChecker.*;

public class IICVResolveChecker implements ConflictResolveChecker.ResolveChecker {

    @Override
    public boolean verifyExpectation(XdcrConflict xdcrExpected, List<XdcrConflict> xdcrActuals, CsvConflictExporter exporter) throws JSONException {
        if (xdcrExpected.getActionTypeEnum().equals(ACTION_TYPE.I) &&
                xdcrExpected.getConflictTypeEnum().equals(CONFLICT_TYPE.CNST)) {

            if (xdcrActuals.stream().anyMatch(
                    xdcrConflict -> xdcrConflict.getActionTypeEnum().equals(ACTION_TYPE.I))) {
                logXdcrVerification("Verifying IICV resolution: cid %d, rid %d on table %s",
                        xdcrExpected.getCid(), xdcrExpected.getRid(), xdcrExpected.getTableName());
                checkIICVResolution(xdcrExpected, xdcrActuals);
                exporter.export(xdcrExpected, xdcrActuals, CONFLICT_TYPE.IICV);
                return true;
            }
        }

        if (xdcrExpected.getActionTypeEnum().equals(ACTION_TYPE.I) &&
                xdcrExpected.getConflictTypeEnum().equals(CONFLICT_TYPE.IICV)) {

            if (xdcrActuals.isEmpty()) {
                return true;
            }

            failStop("Found IICV conflict records: " + xdcrActuals);
        }

        return false;
    }

    public void checkIICVResolution(XdcrConflict xdcrExpected, List<XdcrConflict> xdcrActuals) throws JSONException {
        checkEquals("Incorrect number of conflict records:  expected %d, actual %d", 2, xdcrActuals.size());
        for (XdcrConflict xdcrActual : xdcrActuals) {
            if (xdcrActual.getRowTypeEnum().equals(XdcrConflict.ROW_TYPE.NEW)) {
                checkEquals("Mismatched ACTION_TYPE: expected %s, actual %s", xdcrExpected.getActionType(), xdcrActual.getActionType());
                checkEquals("Mismatched CONFLICT_TYPE: expected %s, actual %s", xdcrExpected.getConflictType(), xdcrActual.getConflictType());
                checkEquals("Mismatched DECISION: expected %s, actual %s", xdcrExpected.getDecision(), xdcrActual.getDecision());
                checkEquals("Mismatched Divergence: expected %s, actual %s", xdcrExpected.getDivergenceType(), xdcrActual.getDivergenceType());
                checkEquals("Unexpected matching key column: expected %s, actual %s",
                        xdcrExpected.getKey(), toByteArray(xdcrActual.getTuple().getString("KEY")));
            }
            else if (xdcrActual.getRowTypeEnum().equals(XdcrConflict.ROW_TYPE.EXT)) {
                checkEquals("Mismatched ACTION_TYPE: expected %s, actual %s", xdcrExpected.getActionType(), xdcrActual.getActionType());
                checkEquals("Mismatched CONFLICT_TYPE: expected %s, actual %s", xdcrExpected.getConflictType(), xdcrActual.getConflictType());
                checkEquals("Mismatched DECISION: expected %s, actual %s", xdcrExpected.getDecision(), xdcrActual.getDecision());
                checkEquals("Mismatched Divergence: expected %s, actual %s", xdcrExpected.getDivergenceType(), xdcrActual.getDivergenceType());
                checkEquals("Mismatched key column: expected %s, actual %s",
                        xdcrExpected.getKey(), toByteArray(xdcrActual.getTuple().getString("KEY")));
                checkEquals("Mismatched value column: expected %s, actual %s",
                        xdcrExpected.getValue(), toByteArray(xdcrActual.getTuple().getString("VALUE")));
            }
            else {
                failStop("Unrecognized row_type: " + xdcrActual.getRowType());
            }
        }
    }
}
