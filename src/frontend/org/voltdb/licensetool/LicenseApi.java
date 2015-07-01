/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.licensetool;

import java.io.File;
import java.util.Calendar;

/**
 * The shared interface used to interact with license tool
 * functionality implemented in the commercial repository.
 */
public interface LicenseApi {
    public boolean initializeFromFile(File license);
    public boolean isTrial();
    public int maxHostcount();
    public Calendar expires();
    public boolean verify() throws LicenseException;
    public boolean isDrReplicationAllowed();
    public boolean isCommandLoggingAllowed();
}
