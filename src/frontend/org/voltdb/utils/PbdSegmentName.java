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

package org.voltdb.utils;

import java.io.File;
import java.text.MessageFormat;

import org.voltcore.logging.VoltLogger;

/**
 * Utility class for generating and parsing the names of segment files
 * <p>
 * File name structure = "nonce_currentCounter_previousCounter[_quarantine].pbd"<br>
 * Where:
 * <ul>
 * <li>currentCounter = Value of monotonic counter at PBD segment creation
 * <li>previousCounter = Value of monotonic counter at previous PBD segment creation
 * <li>quarantine = A flag at the end of the file which indicates if this segment was quarantined
 * </ul>
 */
public final class PbdSegmentName {
    // 1(nonce) + 1(_) + 15(segmentId) + 1(_) + 4(.pbd)
    private static final int MINIMUM_LENGTH = 22;
    static final String PBD_SUFFIX = ".pbd";
    private static final String PBD_QUARANTINED = "_q";
    private static final MessageFormat FORMAT = new MessageFormat(
            "{0}_{1,number,000000000000000}{2}" + PBD_SUFFIX);

    private static final PbdSegmentName NOT_PBD = new PbdSegmentName(Result.NOT_PBD);
    private static final PbdSegmentName INVALID_NAME = new PbdSegmentName(Result.INVALID_NAME);

    /** The result of parsing a file name. The other fields are only valid if the result is {@link Result#OK} */
    public final Result m_result;
    /** File which was parsed to generate this segment name instance */
    public final File m_file;
    /** The nonce of the segment */
    public final String m_nonce;
    /** The id of this segment */
    public final long m_id;
    /** Whether or not this PBD segment was marked quarantined */
    public final boolean m_quarantined;

    public static String createName(String nonce, long id, boolean quarantine) {
        return FORMAT.format(new Object[] { nonce, id, quarantine ? PBD_QUARANTINED : "" });
    }

    public static PbdSegmentName asQuarantinedSegment(VoltLogger logger, File file) {
        PbdSegmentName current = parseFile(logger, file);
        if (current.m_result != Result.OK) {
            throw new IllegalArgumentException("File is not a valid pbd: " + file);
        }

        if (current.m_quarantined) {
            throw new IllegalArgumentException("File is already quarantined: " + file);
        }

        File quarantinedFile = new File(file.getParentFile(),
                createName(current.m_nonce, current.m_id, true));
        return new PbdSegmentName(quarantinedFile, current.m_nonce, current.m_id, true);
    }

    public static PbdSegmentName parseFile(VoltLogger logger, File file) {
        String fileName = file.getName();
        if (!fileName.endsWith(PBD_SUFFIX)) {
            if (logger.isTraceEnabled()) {
                logger.trace("File " + fileName + " is not a pbd");
            }
            return NOT_PBD;
        }

        if (fileName.length() < MINIMUM_LENGTH) {
            if (logger.isTraceEnabled()) {
                logger.trace("File " + fileName + " is not long enough for valid PBD");
            }
            return INVALID_NAME;
        }

        int endOfId = fileName.length() - PBD_SUFFIX.length();
        boolean quarantined = false;
        if (fileName.regionMatches(endOfId - PBD_QUARANTINED.length(), PBD_QUARANTINED, 0,
                PBD_QUARANTINED.length())) {
            quarantined = true;
            endOfId -= PBD_QUARANTINED.length();
        }

        int startOfId = fileName.lastIndexOf('_', endOfId - 1);
        if (startOfId <= 0) {
            if (logger.isTraceEnabled()) {
                logger.trace("File " + fileName + " does not have a _ in it for id");
            }
            return INVALID_NAME;
        }

        long id;
        try {
            id = Long.parseLong(fileName.substring(startOfId + 1, endOfId));
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse IDs in " + fileName);
            return INVALID_NAME;
        }

        return new PbdSegmentName(file, fileName.substring(0, startOfId), id, quarantined);
    }

    private PbdSegmentName(Result result) {
        m_result = result;
        m_file = null;
        m_nonce = null;
        m_id = -1;
        m_quarantined = false;
    }

    private PbdSegmentName(File file, String nonce, long id, boolean quarantined) {
        m_result = Result.OK;
        m_file = file;
        m_nonce = nonce;
        m_id = id;
        m_quarantined = quarantined;
    }

    public enum Result {
        OK, NOT_PBD, INVALID_NAME;
    }
}
