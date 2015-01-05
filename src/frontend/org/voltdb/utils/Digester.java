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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;

public class Digester {

    final public static byte [] sha1(final byte buf[]) {
        Preconditions.checkArgument(buf !=null, "specified null buffer");
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
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
        Preconditions.checkArgument(str != null, "specified null string");
        return sha1AsBase64(str.getBytes(Charsets.UTF_8));
    }

    final public static String sha1AsHex(final byte buf []) {
        return Encoder.hexEncode(sha1(buf));
    }

    final public static String sha1AsHex(final String str) {
        Preconditions.checkArgument(str != null, "specified null string");
        return sha1AsHex(str.getBytes(Charsets.UTF_8));
    }

}
