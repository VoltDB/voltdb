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

package org.voltdb.deploy;

import java.util.Optional;


public class ConfigProberException extends DeployBaseException {

    private static final long serialVersionUID = -8673791255147953438L;

    public static Optional<ConfigProberException> isCauseFor(Throwable t) {
        Optional<ConfigProberException> opt = Optional.empty();
        while (t != null && !opt.isPresent()) {
            if (t instanceof ConfigProberException) {
                opt = Optional.of((ConfigProberException)t);
            }
            t = t.getCause();
        }
        return opt;
    }

    public ConfigProberException() {
    }

    public ConfigProberException(String format, Object... args) {
        super(format, args);
    }

    public ConfigProberException(Throwable cause) {
        super(cause);
    }

    public ConfigProberException(String format, Throwable cause,
            Object... args) {
        super(format, cause, args);
    }
}
