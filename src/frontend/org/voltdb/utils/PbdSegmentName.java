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
import org.voltdb.RealVoltDB;

/**
 * Utility class for generating and parsing the names of segment files
 * <p>
 * File name structure = "nonce_currentCounter_previousCounter[_quarantine]_version.pbd"<br>
 * Where:
 * <ul>
 * <li>currentCounter = Value of monotonic counter at PBD segment creation
 * <li>previousCounter = Value of monotonic counter at previous PBD segment creation
 * <li>quarantine = A flag at the end of the file which indicates if this segment was quarantined
 * <li>version = A version string indicating version of filename
 * </ul>
 */
public final class PbdSegmentName {
    private static final int MINIMUM_LENGTH = 28;
    static final String PBD_SUFFIX = ".pbd";
    private static final String PBD_QUARANTINED = "_q";
    private static final MessageFormat FORMAT = new MessageFormat(
            "{0}_{1,number,0000000000}_{2,number,0000000000}{3}_{4}" + PBD_SUFFIX);
    static final String VERSION_STR = getVersionString();

    private static final PbdSegmentName NOT_PBD = new PbdSegmentName(Result.NOT_PBD);
    private static final PbdSegmentName INVALID_NAME = new PbdSegmentName(Result.INVALID_NAME);
    private static final PbdSegmentName INVALID_VERSION = new PbdSegmentName(Result.INVALID_VERSION);

    /** The result of parsing a file name. The other fields are only valid if the result is {@link Result#OK} */
    public final Result m_result;
    /** File which was parsed to generate this segment name instance */
    public final File m_file;
    /** The nonce of the segment */
    public final String m_nonce;
    /** The id of this segment */
    public final long m_id;
    /** The id of the previous segment */
    public final long m_prevId;
    /** Whether or not this PBD segment was marked quarantined */
    public final boolean m_quarantined;

    public static String createName(String nonce, long id, long prevId, boolean quarantine) {
        return FORMAT.format(new Object[] { nonce, id, prevId, quarantine ? PBD_QUARANTINED : "", VERSION_STR });
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
                createName(current.m_nonce, current.m_id, current.m_prevId, true));
        return new PbdSegmentName(quarantinedFile, current.m_nonce, current.m_id, current.m_prevId, true);
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

        int endOfName = fileName.length() - PBD_SUFFIX.length();
        int startOfVersion = fileName.lastIndexOf('_', endOfName - 1);
        if (startOfVersion <= 0) {
            if (logger.isTraceEnabled()) {
                logger.trace("File " + fileName + " does not have a _ in it for version");
            }
            return INVALID_NAME;
        }

        if (!fileName.regionMatches(startOfVersion + 1, VERSION_STR, 0, VERSION_STR.length())) {
            if (logger.isTraceEnabled()) {
                logger.trace("File " + fileName + " is not required version: " + VERSION_STR);
            }
            return INVALID_VERSION;
        }

        int endOfPrevId = startOfVersion;
        boolean quarantined = false;
        if (fileName.regionMatches(endOfPrevId - PBD_QUARANTINED.length(), PBD_QUARANTINED, 0,
                PBD_QUARANTINED.length())) {
            quarantined = true;
            endOfPrevId -= PBD_QUARANTINED.length();
        }

        int startOfPrevId = fileName.lastIndexOf('_', endOfPrevId - 1);
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
            prevId = Long.parseLong(fileName.substring(startOfPrevId + 1, endOfPrevId));
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse IDs in " + fileName);
            return INVALID_NAME;
        }

        return new PbdSegmentName(file, fileName.substring(0, startOfId), id, prevId, quarantined);
    }

    private static String getVersionString() {
        String versionNumber = RealVoltDB.extractBuildInfo(null)[0];
        StringBuilder sb = new StringBuilder();
        for (String number : versionNumber.split("\\.")) {
            sb.append(String.format("%02d", Integer.parseInt(number)));
        }
        return sb.toString();
    }

    private PbdSegmentName(Result result) {
        m_result = result;
        m_file = null;
        m_nonce = null;
        m_id = -1;
        m_prevId = -1;
        m_quarantined = false;
    }

    private PbdSegmentName(File file, String nonce, long id, long prevId, boolean quarantined) {
        m_result = Result.OK;
        m_file = file;
        m_nonce = nonce;
        m_id = id;
        m_prevId = prevId;
        m_quarantined = quarantined;
    }

    public enum Result {
        OK, NOT_PBD, INVALID_NAME, INVALID_VERSION;
    }
}
