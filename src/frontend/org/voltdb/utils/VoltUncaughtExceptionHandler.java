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

package org.voltdb.utils;

import java.lang.Thread.UncaughtExceptionHandler;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;

public class VoltUncaughtExceptionHandler implements UncaughtExceptionHandler {

    private final VoltLogger log = new VoltLogger("HOST");

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        VoltDB.crashLocalVoltDB("VoltDB has encountered an unrecoverable error and is exiting."
                + "\nThe log may contain additional information.", true, e);
    }

}
