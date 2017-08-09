/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import org.voltdb.common.Constants;

/**
 * Encode and decode strings and byte arrays to/from hexidecimal
 * string values. This was originally added so binary values could
 * be added to the VoltDB catalogs.
 *
 */
public class Encoder {
    private static final int caseDiff = ('a' - 'A');
    /**
     *
     * @param data A binary array of bytes.
     * @return A hex-encoded string with double length.
     */
    public static String hexEncode(byte[] data) {
        if (data == null)
            return null;

        StringBuilder sb = new StringBuilder();
        for (byte b : data) {
            // hex encoding same way as java.net.URLEncoder.
            char ch = Character.forDigit((b >> 4) & 0xF, 16);
            // to uppercase
            if (Character.isLetter(ch)) {
                ch -= caseDiff;
            }
            sb.append(ch);
            ch = Character.forDigit(b & 0xF, 16);
            if (Character.isLetter(ch)) {
                ch -= caseDiff;
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    /**
     *
     * @param string A string to be hex encoded.
     * @return The double-length hex encoded string.
     */
    public static String hexEncode(String string) {
        // this will need to be less "western" in the future
        return hexEncode(string.getBytes(Constants.UTF8ENCODING));
    }

    /**
     *
     * @param hexString An (even-length) hexadecimal string to be decoded.
     * @return The binary byte array value for the string (half length).
     */
    public static byte[] hexDecode(String hexString) {
        byte[] retval = null;
        final String errorText = "String is not properly hex-encoded.";

        if ((hexString.length() % 2) != 0)
            throw new RuntimeException(errorText);

        try {
            retval = new byte[hexString.length() / 2];
            for (int i = 0; i < retval.length; i++) {
                int value = Integer.parseInt(hexString.substring(2 * i, 2 * i + 2), 16);
                retval[i] = (byte) value;
            }
        }
        catch (IllegalArgumentException exc) {
            // parseInt can throw a NumberFormatException, which is a subclass of
            // IllegalArgumentException, so both kinds of failure come here.
            throw new RuntimeException(errorText);
        }
        return retval;
    }

    /**
     *
     * @param hexString A string of hexidecimal chars of even length.
     * @return A string value de-hexed from the input.
     */
    public static String hexDecodeToString(String hexString) {
        byte[] decodedValue = hexDecode(hexString);
        return new String(decodedValue, Constants.UTF8ENCODING);
    }

    public static boolean isHexEncodedString(String hexString) {
        if ((hexString.length() % 2) != 0)
            return false;
        try {
            for (int i = 0; i < hexString.length(); i++) {
                int value = Integer.parseInt(hexString.substring(i, i + 1), 16);
                if ((value < 0) || (value > 15)) return false;
            }
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static String base64Encode(String string) {
        try {
            final byte[] inBytes = string.getBytes(Constants.UTF8ENCODING);
            return Base64.encodeToString(inBytes, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String base64Encode(byte[] bytes) {
        return Base64.encodeToString(bytes, false);
    }

    public static byte[] base64Decode(String string) {
        return Base64.decodeFast(string);
    }

    public static String base64DecodeToString(String string) {
        byte[] decodedValue = base64Decode(string);
        return new String(decodedValue, Constants.UTF8ENCODING);
    }
}
