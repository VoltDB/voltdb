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

package org.voltdb.modular;

import java.util.Arrays;
import java.util.IllegalFormatConversionException;
import java.util.MissingFormatArgumentException;
import java.util.Optional;
import java.util.UnknownFormatConversionException;


public class ModularException extends RuntimeException {

    private static final long serialVersionUID = 5551793607542563976L;

    public ModularException() {
    }

    public ModularException(String format, Object...args) {
        super(format(format, args));
    }

    public ModularException(Throwable cause) {
        super(cause);
    }

    public ModularException(String format, Throwable cause, Object...args) {
        super(format(format, args), cause);
    }

    static protected String format(String format, Object...args) {
        String formatted = null;
        try {
            formatted = String.format(format, args);
        } catch (MissingFormatArgumentException|IllegalFormatConversionException|
                UnknownFormatConversionException ignoreThem) {
        }
        finally {
            if (formatted == null) {
                formatted = "Format: " + format + ", arguments: " + Arrays.toString(args);
            }
        }
        return formatted;
    }

    public static Optional<ModularException> isCauseFor(Throwable t) {
        Optional<ModularException> opt = Optional.empty();
        while (t != null && !opt.isPresent()) {
            if (t instanceof ModularException) {
                opt = Optional.of((ModularException)t);
            }
            t = t.getCause();
        }
        return opt;
    }

}
