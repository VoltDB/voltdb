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

package org.voltcore.common;

public class Constants
{
    // The default heartbeat timeout value
    public static final int DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 90;
    public static final String VOLT_TMP_DIR = "volt.tmpdir";
    public static final int DEFAULT_INTERNAL_PORT = 3021;
    public static final int DEFAULT_ZK_PORT = 7181;
    public static final String DEFAULT_INTERNAL_INTERFACE = "";
}
