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

import java.util.Stack;

import org.voltdb.sqlparser.syntax.grammar.IOperator;
import org.voltdb.sqlparser.syntax.grammar.ISemantino;
import org.voltdb.sqlparser.syntax.symtab.IExpressionParser;
import org.voltdb.sqlparser.syntax.symtab.IParserFactory;
import org.voltdb.sqlparser.syntax.symtab.ISymbolTable;
import org.voltdb.sqlparser.syntax.symtab.ITable;
import org.voltdb.sqlparser.syntax.symtab.IType;

public class ExpressionParser implements IExpressionParser {
    private final Stack<Semantino> m_semantinoStack = new Stack<Semantino>();
    private final IParserFactory m_factory;
    private final ISymbolTable   m_symbolTable;

    public ExpressionParser(IParserFactory aFactory, ISymbolTable aSymbolTable) {
        m_factory = aFactory;
        m_symbolTable = aSymbolTable;
    }
    @Override
    public void pushSemantino(ISemantino aSemantino) {
        assert(aSemantino instanceof Semantino);
        m_semantinoStack.push((Semantino)aSemantino);
    }

    @Override
    public ISemantino popSemantino() {
        assert m_semantinoStack.isEmpty() == false;
        return m_semantinoStack.pop();
    }

    public boolean isEmpty() {
        return m_semantinoStack.size() == 0;
    }

    @Override
    public Semantino getSemantinoMath(IOperator aOperator,
                                       ISemantino aLeftoperand,
                                       ISemantino aRightoperand) {
        ISemantino[] converted = m_factory.tuac(aLeftoperand, aRightoperand, m_symbolTable);


        return new Semantino((Type) converted[0].getType(),
                            m_factory.makeBinaryAST(aOperator,
                                                    converted[0],
                                                    converted[1]));
    }

    @Override
    public ISemantino getColumnSemantino(String aColumnName,
                                         String aTableName) {
        String realTableName;
        String tableAlias;
        ITable itbl;
        if (aTableName != null) {
            tableAlias = aTableName;
        } else {
            tableAlias = m_symbolTable.getTableAliasByColumn(aColumnName);
        }
        itbl = m_symbolTable.getTable(tableAlias);
        if (itbl == null) {
            return null;
        }
        realTableName = itbl.getName();
        Column col = (Column) itbl.getColumnByName(aColumnName);
        Type colType = col.getType();
        return new Semantino(colType,
                            m_factory.makeColumnRef(realTableName,
                                                    tableAlias,
                                                    aColumnName));
    }

    @Override
    public ISemantino getConstantSemantino(Object aValue, IType aType) {
        return new Semantino(aType, m_factory.makeConstantAST(aType, aValue));
    }

    @Override
    public ISemantino getSemantinoCompare(IOperator aOperator,
                                          ISemantino aLeftoperand,
                                          ISemantino aRightoperand) {
        ISemantino[] converted = m_factory.tuac(aLeftoperand, aRightoperand, m_symbolTable);
        if (converted == null) {
            return Semantino.getErrorSemantino();
        }
        return new Semantino((Type) m_factory.getBooleanType(),
                            m_factory.makeBinaryAST(aOperator,
                                                    converted[0],
                                                    converted[1]));
    }
    @Override
    public ISemantino getSemantinoBoolean(IOperator aOperator,
            ISemantino aLeftoperand, ISemantino aRightoperand) {
        if (aLeftoperand.getType().isBooleanType() == false
                || aRightoperand.getType().isBooleanType() == false) {
            return Semantino.getErrorSemantino();
        }
        return new Semantino((Type) aRightoperand.getType(),
                            m_factory.makeBinaryAST(aOperator,
                                                    aLeftoperand,
                                                    aRightoperand));
    }
    @Override
    public ISemantino getSemantinoBoolean(IOperator aOperator, ISemantino aOperand) {
        if (aOperand.getType().isBooleanType() == false) {
            return Semantino.getErrorSemantino();
        }
        return new Semantino((Type) aOperand.getType(),
                            m_factory.makeUnaryAST(aOperand.getType(), aOperator, aOperand));
    }

}
