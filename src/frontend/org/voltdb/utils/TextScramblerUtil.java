/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import com.google.common.base.Throwables;

public final class TextScramblerUtil {

    static class Holder {
        static {
            Class<?> scramblerClazz = UnimplementedScrambler.class;
            try {
                scramblerClazz = Class.forName("org.voltdb.utils.OnusScrambler");
            } catch(ClassNotFoundException ignoreIt) {
            }
            TextScrambler instance = null;
            try {
                instance = (TextScrambler) scramblerClazz.newInstance();
            } catch (Exception ex) {
                Throwables.propagate(ex);
            }
            scrambler = instance;
        }
        private final static TextScrambler scrambler;
    }

     public final static String scramble(String text) {
         return Holder.scrambler.scramble(text);
     }

     public final static String unscramble(String text) {
         return Holder.scrambler.unscramble(text);
     }
}
