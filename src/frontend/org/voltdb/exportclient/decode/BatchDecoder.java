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

package org.voltdb.exportclient.decode;

import java.util.List;
import org.voltdb.VoltType;


public interface BatchDecoder<T,E extends Exception>  {

    public void add(long generation, String tableName, List<VoltType> types, List<String> names, Object [] fields) throws E;

    public T harvest(long generation);

    public void discard(long generation);

    public static class BulkException extends RuntimeException {

        private static final long serialVersionUID = 9211551790333384076L;

        public BulkException() {
            super();
        }

        public BulkException(String message, Throwable cause) {
            super(message, cause);
        }

        public BulkException(String message) {
            super(message);
        }

        public BulkException(Throwable cause) {
            super(cause);
        }
    }
}
