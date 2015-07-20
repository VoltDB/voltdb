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
 package org.voltdb.sqlparser.semantics.symtab;

import org.voltdb.sqlparser.syntax.grammar.INeutrino;
import org.voltdb.sqlparser.syntax.symtab.IAST;

public class Neutrino implements INeutrino {

        private Type m_type;
        private IAST m_ast;

        @Override
        public boolean isBooleanExpression() {
                if (m_type.getClass() == BooleanType.class)
                        return true;
                else
                        return false;
        }

        public Neutrino (Type newtype, IAST xml) {
                this.m_type = newtype;
                this.m_ast = xml;
        }

        public Type getType() {
                return m_type;
        }

        public Class<?> getSuperType() {
                return m_type.getSuperType();
        }

        public IAST getAST() {
            return m_ast;
        }

}
