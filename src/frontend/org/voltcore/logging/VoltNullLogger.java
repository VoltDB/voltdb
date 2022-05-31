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

package org.voltcore.logging;

/**
 * A VoltLogger that doesn't log. Kinda dumb, but I needed it.
 */
public class VoltNullLogger extends VoltLogger {

    public VoltNullLogger() {
        super(new CoreNullLogger());
    }

    static public class CoreNullLogger implements CoreVoltLogger {
        @Override
        public boolean isEnabledFor(Level level) {
            return false;
        }

        @Override
        public void log(Level level, Object message, Throwable t) {}

        @Override
        public long getLogLevels(VoltLogger[] loggers) {
            return 0;
        }

        @Override
        public void setLevel(Level level) {}
    }
}
