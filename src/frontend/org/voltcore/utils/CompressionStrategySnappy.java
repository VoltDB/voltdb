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

package org.voltcore.utils;

import java.io.IOException;

import org.xerial.snappy.Snappy;


/**
 * Utility class for accessing a variety of com.sun.misc.Unsafe stuff
 */
public class CompressionStrategySnappy extends CompressionStrategy {

    public static final CompressionStrategySnappy INSTANCE = new CompressionStrategySnappy();

    private CompressionStrategySnappy() {}

    @Override
    public byte[] compress(byte data[]) throws IOException {
        return Snappy.compress(data);
    }

    @Override
    public byte[] uncompress(byte data[]) throws IOException {
        return Snappy.uncompress(data);
    }
}
