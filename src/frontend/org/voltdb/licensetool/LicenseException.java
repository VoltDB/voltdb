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

/*
 * All the various cryptographic and IO exceptions, and there are
 * many, that bubble up are wrapped in this license exception
 * and allowed to escape the license checker.
 */
public class LicenseException extends Exception {
    private static final long serialVersionUID = 8699544709857898752L;
    LicenseException(Exception ex) {
        super(ex);
    }
    LicenseException(String message) {
        super(message);
    }
}
