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
package org.voltdb.sqlparser.syntax.symtab;

import org.voltdb.sqlparser.syntax.grammar.IOperator;
import org.voltdb.sqlparser.syntax.grammar.ISemantino;

public interface IExpressionParser {

    /**
     * The expression parser maintains a stack of semantinos.
     * This pops the top element from the stack.
     *
     * @return
     */
    ISemantino popSemantino();

    /**
     * The expression parser maintains a stack of semantinos.
     * This pushes a semantino onto the stack.
     *
     * @param aSemantino
     */
    void pushSemantino(ISemantino aSemantino);

    ISemantino getColumnSemantino(String aColumnName, String aTableName);

    ISemantino getConstantSemantino(Object value, IType type);

    ISemantino getSemantinoMath(IOperator aOperator, ISemantino aLeftoperand,
            ISemantino aRightoperand);

    ISemantino getSemantinoCompare(IOperator aOperator, ISemantino aLeftoperand,
            ISemantino aRightoperand);

    /**
     * Create a Semantino for a binary operation.  This would be OR or AND.  The
     * IOperator knows which one it is.
     *
     * @param aOperator
     * @param aLeftoperand
     * @param aRightoperand
     * @return
     */
    ISemantino getSemantinoBoolean(IOperator aOperator, ISemantino aLeftoperand,
            ISemantino aRightoperand);

    /**
     * Create a Semantino for a unary boolean operation.  There is only one
     * of these, NOT, but we drag it along anyway.
     *
     * @param op
     * @param operand
     * @return
     */
    ISemantino getSemantinoBoolean(IOperator op, ISemantino operand);


}
