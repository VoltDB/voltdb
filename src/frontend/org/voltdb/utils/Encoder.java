/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.zip.GZIPOutputStream;

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
     * @param string A (latin) string to be hex encoded.
     * @return The double-length hex encoded string.
     */
    public static String hexEncode(String string) {
        byte[] strbytes = {};
        try {
            // this will need to be less "western" in the future
            strbytes = string.getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return hexEncode(strbytes);
    }

    /**
     *
     * @param hexString An (even-length) hexidecimal string to be decoded.
     * @return The binary byte array value for the string (half length).
     */
    public static byte[] hexDecode(String hexString) {
        assert (hexString.length() % 2) == 0;

        hexString = hexString.toUpperCase();
        byte[] retval = new byte[hexString.length() / 2];
        for (int i = 0; i < retval.length; i++) {
            retval[i] = (byte) Integer.parseInt(hexString.substring(2 * i, 2 * i + 2), 16);
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
        String retval = null;
        try {
            retval = new String(decodedValue, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return retval;
    }

    public static String compressAndBase64Encode(String string) {
        try {
            byte[] inBytes = string.getBytes("UTF-8");
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int)(string.length() * 0.7));
            GZIPOutputStream gzos = new GZIPOutputStream(baos);
            gzos.write(inBytes);
            gzos.close();
            byte[] outBytes = baos.toByteArray();
            return Base64.encodeBytes(outBytes);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static byte[] compressAndBase64EncodeToBytes(String string) {
        try {
            byte[] inBytes = string.getBytes("UTF-8");
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int)(string.length() * 0.7));
            GZIPOutputStream gzos = new GZIPOutputStream(baos);
            gzos.write(inBytes);
            gzos.close();
            byte[] outBytes = baos.toByteArray();
            return Base64.encodeBytesToBytes(outBytes);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static byte[] compressAndBase64EncodeToBytes(byte inBytes[]) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int)(inBytes.length * .7));
            GZIPOutputStream gzos = new GZIPOutputStream(baos);
            gzos.write(inBytes);
            gzos.close();
            byte[] outBytes = baos.toByteArray();
            return Base64.encodeBytesToBytes(outBytes);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static String base64Encode(String string) {
        try {
            final byte[] inBytes = string.getBytes("UTF-8");
            return Base64.encodeBytes(inBytes);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static byte[] base64EncodeToBytes(String string) {
        try {
            final byte[] inBytes = string.getBytes("UTF-8");
            return Base64.encodeBytesToBytes(inBytes);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static byte[] base64EncodeToBytes(byte bytes[]) {
        try {
            return Base64.encodeBytesToBytes(bytes);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static String decodeBase64AndDecompress(String string) {
        try {
            byte[] bytes = Base64.decode(string);
            return new String(bytes, "UTF-8");
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static byte[] decodeBase64AndDecompressToBytes(byte inbytes[]) {
        try {
            return Base64.decodeAndGUnzip(inbytes);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static byte[] decodeBase64AndDecompressToBytes(String string) {
        try {
            return Base64.decode(string);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static String decodeBase64(String string) {
        try {
            return new String(Base64.decode(string, Base64.DONT_GUNZIP), "UTF-8");
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static byte[] decodeBase64ToBytes(String string) {
        try {
            return Base64.decode(string, Base64.DONT_GUNZIP);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static byte[] decodeBase64ToBytes(byte bytes[]) {
        try {
            return Base64.decode(bytes);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
