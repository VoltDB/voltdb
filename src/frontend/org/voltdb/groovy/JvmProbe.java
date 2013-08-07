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

package org.voltdb.groovy;

import org.voltdb.compiler.Language;

public class JvmProbe {
    public final static float version = Float.parseFloat(System.getProperty("java.specification.version"));

    public final static boolean mayLoadGroovy() {
        return version >= 1.69;
    }

    static final Language.Visitor<Boolean, Void> mayLoadVtor = new Language.Visitor<Boolean, Void>() {
        @Override
        public Boolean visitJava(Void p) {
            return true;
        }
        @Override
        public Boolean visitGroovy(Void p) {
            return mayLoadGroovy();
        }
    };

    public final static boolean mayLoad( final Language lang) {
        return lang.accept(mayLoadVtor, (Void)null);
    }
}
