/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.planner;

/**
 * The default (catch-all) SQL handler for query types without a designated handler.
 * @since 8.4
 * @author Yiqun Zhang
 */
public class DefaultSqlHandler extends SqlHandler {

    /**
     * The singleton handler instance for the {@link DefaultSqlHandler}.
     */
    public static final DefaultSqlHandler INSTANCE = new DefaultSqlHandler();

    private DefaultSqlHandler() {

    }

    @Override
    public void doAction(SqlTask task) {

    }

}
