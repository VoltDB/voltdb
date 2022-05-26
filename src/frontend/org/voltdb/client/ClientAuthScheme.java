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

package org.voltdb.client;

import java.util.EnumSet;

/**
 * This is enum for Hash schemes we support.
 */
public enum ClientAuthScheme {
    HASH_SHA1, HASH_SHA256, SPNEGO;

    private final static EnumSet<ClientAuthScheme> hashedOnes =
            EnumSet.of(HASH_SHA1, HASH_SHA256);

    //Modify this if you add a new scheme.
    private static final ClientAuthScheme theList[] = values();

    public int getValue() {
        return ordinal();
    }

    public boolean isHashed() {
        return hashedOnes.contains(this);
    }

    public final static ClientAuthScheme get(int i) {
        if (i < 0 || i >= theList.length) {
            throw new IllegalArgumentException("Invalid Hash Scheme");
        }
        return theList[i];
    }

    public final static ClientAuthScheme getByUnencodedLength(int i) {
        switch (i) {
            case 20: return HASH_SHA1;
            case 32: return HASH_SHA256;
            case 0: return SPNEGO;
            default: throw new IllegalArgumentException("Invalid Hash Scheme for given length: " + i);
        }
    }

    public final static int getDigestLength(ClientAuthScheme scheme) {
        switch (scheme) {
            case HASH_SHA1 : return 20;
            case HASH_SHA256 : return 32;
            case SPNEGO : return 0;
            default : throw new IllegalArgumentException("Invalid Hash Scheme for Authentication.");
        }
    }

    public final static int getHexencodedDigestLength(ClientAuthScheme scheme) {
        switch (scheme) {
            case HASH_SHA1 : return 40;
            case HASH_SHA256 : return 64;
            case SPNEGO : return 0;
            default : throw new IllegalArgumentException("Invalid Hash Scheme for Authentication.");
        }
    }

    public final static String getDigestScheme(ClientAuthScheme scheme) {
        switch (scheme) {
            case HASH_SHA1 : return "SHA-1";
            case HASH_SHA256 : return "SHA-256";
            case SPNEGO : return "NONE";
            default : throw new IllegalArgumentException("Invalid Hash Digest Scheme for Authentication.");
        }
    }

}
