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

package org.voltdb.importer;

import org.voltcore.logging.Level;

/**
 * Interface for providing logging support to Importer implementations.
 * @author jcrump
 */
public interface ImporterLogger {

    public void rateLimitedLog(Level level, Throwable cause, String format, Object... args);
    public void info(Throwable t, String msgFormat, Object... args);
    public void warn(Throwable t, String msgFormat, Object... args);
    public void error(Throwable t, String msgFormat, Object... args);
    public void debug(Throwable t, String msgFormat, Object... args);
    public boolean isDebugEnabled();
}
