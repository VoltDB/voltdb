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

package org.voltdb.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.VoltType;
import org.voltdb.common.Constants;

/**
 * Configurable helper for parsing ByteBuffers.
 * Enforces reasonableness constraints on parsed values.
 */
public class ByteBufferUtil {
    public static class StringReader {

        private static final long USUAL_NULL_STRING_LENGTH = -1;
        private static final long DISALLOWING_NULL_STRING_LENGTH = Long.MIN_VALUE;

        // Null string length is purposely wider than int so that it
        // can be set to NO_NULLS -- out of range for int values --
        // to prohibit null strings.
        // That way, a -1 length will give a consistent "value out of range" error.
        private final long NULL_STRING_LENGTH;
        private final int MIN_STRING_LENGTH;
        private final int MAX_STRING_LENGTH;
        private final String EXCEPTION_FORMAT_MIN_LENGTH;
        private final String EXCEPTION_FORMAT_MAX_LENGTH;
        private final String EXCEPTION_FORMAT_NOT_AVAILABLE_LENGTH;

        public StringReader(boolean allow_null, int min_len, int max_len,
                String min_complaint, String max_complaint, String high_complaint)
        {
            NULL_STRING_LENGTH = allow_null ?
                    USUAL_NULL_STRING_LENGTH :
                    DISALLOWING_NULL_STRING_LENGTH;
            MIN_STRING_LENGTH = min_len;
            MAX_STRING_LENGTH = max_len;
            EXCEPTION_FORMAT_MIN_LENGTH = min_complaint;
            EXCEPTION_FORMAT_MAX_LENGTH = max_complaint;
            EXCEPTION_FORMAT_NOT_AVAILABLE_LENGTH = high_complaint;
        }

        /// A readString variant that eases the checked exception handling
        /// burden on the client.
        public String readStringNoIOException(ByteBuffer source) {
            try {
                return readString(source);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Read a string in the standard VoltDB way without
         * wrapping the byte buffer.
         * That is, a four byte length, or -1 for null,
         * followed by the bytes of UTF-8 encoded characters.
         * @throws IOException
         */
        public String readString(ByteBuffer source) throws IOException {
            final int len = source.getInt();

            // check for null string
            if (len == NULL_STRING_LENGTH) {
                return null;
            }

            if (len > source.remaining()) {
                int remaining = source.remaining();
                throwAvailableLengthViolation(len, remaining);
            }
            if (len > MAX_STRING_LENGTH) {
                throwMaxLengthViolation(len);
            }
            if (len < MIN_STRING_LENGTH) {
                throwMinLengthViolation(len);
            }
            final byte[] strbytes = new byte[len];
            source.get(strbytes);
            return new String(strbytes, Constants.UTF8ENCODING);
        }

        protected void throwMaxLengthViolation(final int len) throws IOException {
            String complaint = "";
            try {
                complaint = String.format(EXCEPTION_FORMAT_MAX_LENGTH, len);
            }
            catch (Exception ignoreAsMinor) { }

            throw new IOException(complaint);
        }

        protected void throwMinLengthViolation(final int len) throws IOException {
            String complaint = "";
            try {
                complaint = String.format(EXCEPTION_FORMAT_MIN_LENGTH, len);
            }
            catch (Exception ignoreAsMinor) { }

            throw new IOException(complaint);
        }

        protected void throwAvailableLengthViolation(final int len, int remaining)
                throws IOException {
            String complaint = "";
            try {
                complaint = String.format(EXCEPTION_FORMAT_NOT_AVAILABLE_LENGTH,
                        len, remaining);
            }
            catch (Exception ignoreAsMinor) { }

            throw new IOException(complaint);
        }
    }

    private static StringReader m_nonNullSymbolReader = null;
    private static StringReader m_decimalReader = null;
    private static StringReader m_arbitraryReader = null;
    private static StringReader m_varcharReader = null;

    public static String readArbitraryString(ByteBuffer source) throws IOException {
        if (m_arbitraryReader == null) {
            boolean allow_nulls = true;
            int min_len = 0;
            int max_len = Integer.MAX_VALUE; // more than reasonable
            String min_complaint = "The serialized length %d for a string was negative";
            String max_complaint = ""; // N/A
            String high_complaint = "The serialized length %d for a string exceeded the remaining buffer length %d";
            m_arbitraryReader =
                    new StringReader(allow_nulls, min_len, max_len,
                            min_complaint, max_complaint, high_complaint);
        }
        return m_arbitraryReader.readString(source);
    }

    public static String readNonNullSymbolString(ByteBuffer source) throws IOException {

        if (m_nonNullSymbolReader == null) {
            boolean allow_nulls = false;
            int min_len = 1; // non-optional
            int max_len = 2048; // seems more than reasonable for an identifier.
            String min_complaint = "The serialized length %d for a name string was outside the expected range of " +
                    min_len + " to " + max_len;
            String max_complaint = min_complaint;
            String high_complaint = "The serialized length %d for a name string exceeded the remaining buffer length %d";
            m_nonNullSymbolReader =
                    new StringReader(allow_nulls, min_len, max_len,
                            min_complaint, max_complaint, high_complaint);
        }
        return m_nonNullSymbolReader.readString(source);
        }

    public static String readDecimalString(ByteBuffer source) throws IOException {
        if (m_decimalReader == null) {
            // null decimal values are represented by a special value.
            boolean allow_nulls = false; // null string not allowed
            int min_len = 1; // non-optional
            int max_len = 60; // more than reasonable
            String min_complaint = "The serialized length %d for a decimal value string was outside the expected range of " +
                    min_len + " to " + max_len;
            String max_complaint = min_complaint;
            String high_complaint = "The serialized length %d for a decimal value string exceeded the remaining buffer length %d";
            m_decimalReader =
                    new StringReader(allow_nulls, min_len, max_len,
                            min_complaint, max_complaint, high_complaint);
        }
        return m_decimalReader.readString(source);
    }

    public static String readVarchar(ByteBuffer source) throws IOException {
        if (m_varcharReader == null) {
            boolean allow_nulls = true;
            int min_len = 0;
            int max_len = VoltType.MAX_VALUE_LENGTH;
            String min_complaint = "The serialized length %d for a string was negative";
            String max_complaint = ""; // N/A
            String high_complaint = "The serialized length %d for a string exceeded the remaining buffer length %d";
            m_varcharReader =
                    new StringReader(allow_nulls, min_len, max_len,
                            min_complaint, max_complaint, high_complaint);
        }
        return m_varcharReader.readString(source);
    }

}
