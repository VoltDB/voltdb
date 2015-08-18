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
package org.voltdb.sqlparser.semantics.grammar;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.sqlparser.semantics.symtab.Column;
import org.voltdb.sqlparser.semantics.symtab.ExpressionParser;
import org.voltdb.sqlparser.semantics.symtab.Semantino;
import org.voltdb.sqlparser.semantics.symtab.SymbolTable;
import org.voltdb.sqlparser.semantics.symtab.SymbolTable.TablePair;
import org.voltdb.sqlparser.semantics.symtab.Table;
import org.voltdb.sqlparser.syntax.grammar.IOperator;
import org.voltdb.sqlparser.syntax.grammar.ISelectQuery;
import org.voltdb.sqlparser.syntax.grammar.ISemantino;
import org.voltdb.sqlparser.syntax.grammar.Projection;
import org.voltdb.sqlparser.syntax.symtab.IAST;
import org.voltdb.sqlparser.syntax.symtab.IExpressionParser;
import org.voltdb.sqlparser.syntax.symtab.IParserFactory;
import org.voltdb.sqlparser.syntax.symtab.ITable;
import org.voltdb.sqlparser.syntax.symtab.IType;
import org.voltdb.sqlparser.syntax.util.ErrorMessageSet;



public class SelectQuery implements ISelectQuery, IDQLStatement {
    List<Projection> m_projections = new ArrayList<Projection>();

    private ExpressionParser m_expressionParser;
    private SymbolTable m_tables;
    private Semantino m_whereCondition;
    private IAST m_ast;
    private ErrorMessageSet m_errorMessages;

    public SelectQuery(SymbolTable aParent,
                       IParserFactory aFactory,
                       ErrorMessageSet aErrorMessages,
                       int aLineNo,
                       int aColNo) {
        m_whereCondition = null;
        m_tables = new SymbolTable(aParent);
        m_errorMessages = aErrorMessages;
    }

    public List<Projection> getProjections() {
        return m_projections;
    }

    /**
     * This is called when a '*' is in the select list.
     *
     * @param aLineNo
     * @param aColNo
     */
    @Override
    public void addProjection(int     aLineNumber,
                              int     aColumnNumber) {
        m_projections.add(new Projection(aLineNumber, aColumnNumber));
    }

    @Override
    public void addProjection(String aTableName,
                              String aColumnName,
                              String aAlias,
                              int    aLineNo,
                              int    aColNo) {
        m_projections.add(new Projection(aTableName, aColumnName, aAlias, aLineNo, aColNo));
    }

    @Override
    public void addTable(ITable aTable, String aAlias) {
        if (aAlias != null)
            m_tables.addTable((Table)aTable, aAlias);
        else
            m_tables.addTable((Table)aTable, aTable.getName());
    }

    public ITable getTableByName(String aName) {
        return m_tables.getTable(aName);
    }

    @Override
    public void pushSemantino(ISemantino aColumnSemantino) {
            m_expressionParser.pushSemantino(aColumnSemantino);
    }

    @Override
    public ISemantino popSemantino() {
        return m_expressionParser.popSemantino();
    }

    @Override
    public Semantino getSemantinoMath(IOperator aOperator,
                                       ISemantino aLeftoperand,
                                       ISemantino aRightoperand) {
        return m_expressionParser.getSemantinoMath(aOperator, aLeftoperand, aRightoperand);
    }

    @Override
    public ISemantino getSemantinoCompare(IOperator aOperator,
                                       ISemantino aLeftoperand,
                                       ISemantino aRightoperand) {
        return m_expressionParser.getSemantinoCompare(aOperator, aLeftoperand, aRightoperand);
    }

    @Override
    public ISemantino getSemantinoBoolean(IOperator aOperator,
                                       ISemantino aLeftoperand,
                                       ISemantino aRightoperand) {
        return m_expressionParser.getSemantinoBoolean(aOperator, aLeftoperand, aRightoperand);
    }

    @Override
    public ISemantino getColumnSemantino(String aColumnName, String aTableName) {
        return m_expressionParser.getColumnSemantino(aColumnName, aTableName);
    }

    @Override
    public String printProjections() {
        String out = "projections: ";
        for (int i=0;i<m_projections.size();i++) {
                out += "["+m_projections.get(i).toString()+"]";
        }
        return out;
    }

    @Override
    public String printTables() {
        String out = "Tables: ";
        out += m_tables.toString();
        return out;
    }

    @Override
    public boolean hasSemantinos() {
        return !m_expressionParser.isEmpty();
    }

    @Override
    public void setWhereCondition(ISemantino aSemantino) {
        m_whereCondition = (Semantino) aSemantino;
    }

    @Override
    public IAST getWhereCondition() {
        if (m_whereCondition != null) {
            return m_whereCondition.getAST();
        }
        return null;
    }

    @Override
    public SymbolTable getTables() {
        return m_tables;
    }

    @Override
    public void setAST(IAST aMakeQueryAST) {
        m_ast = aMakeQueryAST;
    }

    public IAST getAST() {
        return m_ast;
    }

    @Override
    public boolean validate() {
        if (m_errorMessages.size() > 0) {
            return false;
        }
        return true;
    }

    @Override
    public ISemantino getConstantSemantino(Object value, IType type) {
        return m_expressionParser.getConstantSemantino(value, type);
    }

    @Override
    public final IExpressionParser getExpressionParser() {
        return m_expressionParser;
    }

    @Override
    public final void setExpressionParser(IExpressionParser aExpressionParser) {
        assert(aExpressionParser == null || aExpressionParser instanceof ExpressionParser);
        m_expressionParser = (ExpressionParser)aExpressionParser;
    }

}
