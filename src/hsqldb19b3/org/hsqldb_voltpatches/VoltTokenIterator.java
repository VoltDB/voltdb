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

package org.hsqldb_voltpatches;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class VoltTokenIterator implements Iterator<VoltToken> {

    private final Scanner m_scanner;

    public class ParseException extends RuntimeException {
        ParseException(String msg) {
            super (msg);
        }
    }

    VoltTokenIterator(String input) {
        m_scanner = new Scanner(input);

        assert (m_scanner.token.tokenType == Tokens.X_STARTPARSE);

        m_scanner.scanNext();
    }

    @Override
    public boolean hasNext() {
        return m_scanner.token.tokenType != Tokens.X_ENDPARSE;
    }

    @Override
    public VoltToken next() {

        if (hasNext()) {

            if (m_scanner.getToken().isMalformed) {
                throw new ParseException("Malformed token: "
                        + m_scanner.getToken().tokenString);
            }

            VoltToken nextToken = new VoltToken(m_scanner.token.duplicate());

            m_scanner.scanNext();

            return nextToken;
        }

        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();

    }

}
