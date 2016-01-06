/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.client;

/**
 * This is enum for Hash schemes we support.
 * @author akhanzode
 */
public enum ClientAuthHashScheme {
    HASH_SHA1(0), HASH_SHA256(1);

    //Modify this if you add a new scheme.
    private static final ClientAuthHashScheme theList[] = { ClientAuthHashScheme.HASH_SHA1, ClientAuthHashScheme.HASH_SHA256 };
    private final int value;

    private ClientAuthHashScheme(int v) {
        this.value = v;
    }

    public int getValue() {
        return value;
    }

    public final static ClientAuthHashScheme get(int i) {
        if (i >= theList.length) {
            throw new IllegalArgumentException("Invalid Hash Scheme");
        }
        return theList[i];
    }

    public final static ClientAuthHashScheme getByUnencodedLength(int i) {
        switch (i) {
            case 20: return HASH_SHA1;
            case 32: return HASH_SHA256;
            default: throw new IllegalArgumentException("Invalid Hash Scheme for given length: " + i);
        }
    }

    public final static int getDigestLength(ClientAuthHashScheme scheme) {
        switch (scheme) {
            case HASH_SHA1 : return 20;
            case HASH_SHA256 : return 32;
            default : throw new IllegalArgumentException("Invalid Hash Scheme for Authentication.");
        }
    }

    public final static int getHexencodedDigestLength(ClientAuthHashScheme scheme) {
        switch (scheme) {
            case HASH_SHA1 : return 40;
            case HASH_SHA256 : return 64;
            default : throw new IllegalArgumentException("Invalid Hash Scheme for Authentication.");
        }
    }

    public final static String getDigestScheme(ClientAuthHashScheme scheme) {
        switch (scheme) {
            case HASH_SHA1 : return "SHA-1";
            case HASH_SHA256 : return "SHA-256";
            default : throw new IllegalArgumentException("Invalid Hash Digest Scheme for Authentication.");
        }
    }

}
