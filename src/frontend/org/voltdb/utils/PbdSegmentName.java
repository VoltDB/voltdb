/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.File;
import java.text.MessageFormat;

import org.voltcore.logging.VoltLogger;

/**
 * Utility class for generating and parsing the names of segment files
 * <p>
 * File name structure = "nonce_currentCounter_previousCounter.pbd"<br>
 * Where:
 * <ul>
 * <li>currentCounter = Value of monotonic counter at PBD segment creation
 * <li>previousCounter = Value of monotonic counter at previous PBD segment creation
 * </ul>
 */
public class PbdSegmentName {
    private static final String PBD_SUFFIX = ".pbd";
    private static final MessageFormat FORMAT = new MessageFormat(
            "{0}_{1,number,0000000000}_{2,number,0000000000}" + PBD_SUFFIX);

    private static final PbdSegmentName NOT_PBD = new PbdSegmentName(Result.NOT_PBD);
    private static final PbdSegmentName INVALID_NAME = new PbdSegmentName(Result.INVALID_NAME);

    /** The result of parsing a file name. The other fields are only valid if the result is {@link Result#OK} */
    public final Result m_result;
    /** The nonce of the segment */
    public final String m_nonce;
    /** The id of this segment */
    public final long m_id;
    /** The id of the previous segment */
    public final long m_prevId;

    public static String createName(String nonce, long id, long prevId) {
        return FORMAT.format(new Object[] { nonce, id, prevId });
    }

    public static PbdSegmentName parseFile(VoltLogger logger, File file) {
        return parseName(logger, file.getName());
    }

    public static PbdSegmentName parseName(VoltLogger logger, String fileName) {
        if (!fileName.endsWith(PBD_SUFFIX)) {
            if (logger.isTraceEnabled()) {
                logger.trace("File " + fileName + " is not a pbd");
            }
            return NOT_PBD;
        }
        int fileNameLength = fileName.length() - PBD_SUFFIX.length();
        int startOfPrevId = fileName.lastIndexOf('_');
        if (startOfPrevId <= 0) {
            if (logger.isTraceEnabled()) {
                logger.trace("File " + fileName + " does not have a _ in it for previous id");
            }
            return INVALID_NAME;
        }
        int startOfId = fileName.lastIndexOf('_', startOfPrevId - 1);
        if (startOfId <= 0) {
            if (logger.isTraceEnabled()) {
                logger.trace("File " + fileName + " does not have a _ in it for id");
            }
            return INVALID_NAME;
        }

        long id, prevId;
        try {
            id = Long.parseLong(fileName.substring(startOfId + 1, startOfPrevId));
            prevId = Long.parseLong(fileName.substring(startOfPrevId + 1, fileNameLength));
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse IDs in " + fileName);
            return INVALID_NAME;
        }

        return new PbdSegmentName(fileName.substring(0, startOfId), id, prevId);
    }

    private PbdSegmentName(Result result) {
        this.m_result = result;
        this.m_nonce = null;
        this.m_id = -1;
        this.m_prevId = -1;
    }

    private PbdSegmentName(String m_nonce, long m_id, long m_prevId) {
        this.m_result = Result.OK;
        this.m_nonce = m_nonce;
        this.m_id = m_id;
        this.m_prevId = m_prevId;
    }

    public enum Result {
        OK, NOT_PBD, INVALID_NAME;
    }
}
