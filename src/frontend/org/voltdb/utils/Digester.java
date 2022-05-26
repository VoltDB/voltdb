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

import static com.google_voltpatches.common.base.Preconditions.checkArgument;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import com.google_voltpatches.common.base.Throwables;

public final class Digester {

    private Digester() {
    }

    private final static BigInteger LSB_MASK = new BigInteger(new byte[] {
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255 });

    final public static byte [] sha1(final byte buf[]) {
        checkArgument(buf !=null, "specified null buffer");
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            Throwables.propagate(e);
        }
        md.reset();
        return md.digest(buf);
    }

    final public static byte [] sha256(final byte buf[]) {
        checkArgument(buf !=null, "specified null buffer");
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            Throwables.propagate(e);
        }
        md.reset();
        return md.digest(buf);
    }

    final public static byte [] md5(final byte buf[]) {
        checkArgument(buf !=null, "specified null buffer");
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            Throwables.propagate(e);
        }
        md.reset();
        return md.digest(buf);
    }

    final public static String sha1AsBase64(final byte buf []) {
        return Encoder.base64Encode(sha1(buf));
    }

    final public static String sha1AsBase64(final String str) {
        checkArgument(str != null, "specified null string");
        return sha1AsBase64(str.getBytes(StandardCharsets.UTF_8));
    }

    final public static String sha1AsHex(final byte buf []) {
        return Encoder.hexEncode(sha1(buf));
    }
    final public static String sha256AsHex(final byte buf []) {
        return Encoder.hexEncode(sha256(buf));
    }

    final public static String shaAsHex(final String str) {
        checkArgument(str != null, "specified null string");
        return (sha1AsHex(str.getBytes(StandardCharsets.UTF_8)) + sha256AsHex(str.getBytes(StandardCharsets.UTF_8)));
    }

    final public static BigInteger md5AsBigInt(final byte [] buf) {
        return new BigInteger(1, md5(buf));
    }

    final public static UUID md5AsUUID(final byte [] buf) {
        BigInteger bi = md5AsBigInt(buf);
        return new UUID(bi.shiftRight(64).longValue(), bi.and(LSB_MASK).longValue());
    }

    final public static UUID md5AsUUID(String str) {
        checkArgument(str != null, "specified null string");
        return md5AsUUID(str.getBytes(StandardCharsets.UTF_8));
    }
}
