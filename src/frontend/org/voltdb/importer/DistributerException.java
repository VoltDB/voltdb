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

import com.google_voltpatches.common.base.Optional;

public class DistributerException extends RuntimeException {

    public static Optional<DistributerException> isCauseFor(Throwable t) {
        Optional<DistributerException> opt = Optional.absent();
        while (t != null && !opt.isPresent()) {
            if (t instanceof DistributerException) {
                opt = Optional.of((DistributerException)t);
            }
            t = t.getCause();
        }
        return opt;
    }

    private static final long serialVersionUID = 8301633646561182522L;

    public DistributerException() {
    }

    public DistributerException(String message) {
        super(message);
    }

    public DistributerException(Throwable cause) {
        super(cause);
    }

    public DistributerException(String message, Throwable cause) {
        super(message, cause);
    }

    public DistributerException(String message, Throwable cause,
            boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
