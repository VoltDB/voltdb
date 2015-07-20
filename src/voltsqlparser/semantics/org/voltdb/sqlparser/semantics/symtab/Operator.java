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

import org.voltdb.sqlparser.syntax.grammar.IOperator;

public enum Operator implements IOperator {
 /* Enumeral,     Syntax,  VoltXML Operator,       Syntactic Category */
    SUM(           "+",     "add",                   100),
    MINUS(         "-",     "subtract",              100),
    PRODUCT(       "*",     "multiply",              100),
    DIVIDES(       "/",     "divide",                100),
    EQUALS(        "=",     "equal",                 200),
    NOT_EQUALS(    "!=",    "notequal",              200),
    LESS_THAN(     "<",     "lessthan",              200),
    LESS_EQUALS(   "<=",    "lessthanorequalto",     200),
    GREATER_THAN(  ">",     "greaterthan",           200),
    GREATER_EQUALS(">=",    "greaterthanorequalto",  200),
    BOOLAND(       "and",   "and",                   300),
    BOOLOR(        "or",    "or",                    300),
    BOOLNOT(       "not",   "not",                   300);

    String m_opn;
    String m_voltOpn;
    int m_kind;
    Operator(String aOpn, String voltOpn, int aKind) {
        m_opn = aOpn;
        m_voltOpn = voltOpn;
        m_kind = aKind;
    }

    public String getOperation() {
        return m_opn;
    }
    public String getVoltOperation() {
        return m_voltOpn;
    }

    public boolean isArithmetic() {
        return m_kind == 100;
    }
    public boolean isRelational() {
        return m_kind == 200;
    }
    public boolean isBoolean() {
        return m_kind == 300;
    }
}
