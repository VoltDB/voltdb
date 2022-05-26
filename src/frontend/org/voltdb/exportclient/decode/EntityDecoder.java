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

import java.net.URI;
import java.util.List;

import org.apache.http.entity.AbstractHttpEntity;
import org.voltdb.VoltType;

public abstract class EntityDecoder implements BatchDecoder<AbstractHttpEntity, RuntimeException>{

    static final protected URI UNCHANGED_URI =
            URI.create("http://unchanged.sentinel/__UNCHANGED_SENTINEL__");

    abstract public AbstractHttpEntity getHeaderEntity(long generation, String tableName, List<VoltType> types, List<String> names);
}
