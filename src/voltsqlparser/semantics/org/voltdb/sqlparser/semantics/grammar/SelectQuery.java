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
 package org.voltdb.sqlparser.semantics.grammar;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.voltdb.sqlparser.semantics.grammar.ErrorMessage.Severity;
import org.voltdb.sqlparser.semantics.symtab.Column;
import org.voltdb.sqlparser.semantics.symtab.Semantino;
import org.voltdb.sqlparser.semantics.symtab.SymbolTable;
import org.voltdb.sqlparser.semantics.symtab.Table;
import org.voltdb.sqlparser.semantics.symtab.Type;
import org.voltdb.sqlparser.syntax.grammar.ISemantino;
import org.voltdb.sqlparser.syntax.grammar.IOperator;
import org.voltdb.sqlparser.syntax.grammar.ISelectQuery;
import org.voltdb.sqlparser.syntax.grammar.Projection;
import org.voltdb.sqlparser.syntax.symtab.IAST;
import org.voltdb.sqlparser.syntax.symtab.IParserFactory;
import org.voltdb.sqlparser.syntax.symtab.ITable;
import org.voltdb.sqlparser.syntax.symtab.IType;
import org.voltdb.sqlparser.syntax.util.ErrorMessageSet;



public class SelectQuery implements ISelectQuery, IDQLStatement {
    List<Projection> m_projections = new ArrayList<Projection>();

    private Stack<Semantino> m_semantinoStack = new Stack<Semantino>();
    private SymbolTable m_tables;
    private Semantino m_whereCondition;
    private IParserFactory m_factory;
    private IAST m_ast;
    private ErrorMessageSet m_errorMessages;
    private int m_EndLineNo;
    private int m_EndColNo;

    public SelectQuery(SymbolTable aParent,
                       IParserFactory aFactory,
                       ErrorMessageSet aErrorMessages,
                       int aLineNo,
                       int aColNo) {
        m_whereCondition = null;
        m_factory = aFactory;
        m_tables = new SymbolTable(aParent);
        m_errorMessages = aErrorMessages;
        m_EndLineNo = aLineNo;
        m_EndColNo = aColNo;
    }

    public List<Projection> getProjections() {
        return m_projections;
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
                m_semantinoStack.push((Semantino) aColumnSemantino);
        }

        @Override
        public Semantino popSemantino() {
                Semantino bottom = m_semantinoStack.pop();
                return bottom;
        }

        @Override
        public Semantino getSemantinoMath(IOperator aOperator,
                                           ISemantino aLeftoperand,
                                           ISemantino aRightoperand) {
                ISemantino[] converted = m_factory.tuac(aLeftoperand, aRightoperand);


                return new Semantino((Type) converted[0].getType(),
                                    m_factory.makeBinaryAST(aOperator,
                                                            converted[0],
                                                            converted[1]));
        }

    @Override
    public Semantino getSemantinoCompare(IOperator aOperator,
                                       ISemantino aLeftoperand,
                                       ISemantino aRightoperand) {
                ISemantino[] converted = m_factory.tuac(aLeftoperand, aRightoperand);
                if (converted == null) {
                    return Semantino.getErrorSemantino();
                }
                return new Semantino((Type) m_factory.getBooleanType(),
                                    m_factory.makeBinaryAST(aOperator,
                                                            converted[0],
                                                            converted[1]));
    }

    @Override
    public Semantino getSemantinoBoolean(IOperator aOperator,
                                       ISemantino aLeftoperand,
                                       ISemantino aRightoperand) {
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
    public Semantino getColumnSemantino(String aColumnName, String aTableName) {
        String realTableName;
        String tableAlias;
        Table tbl;
        if (aTableName != null) {
            tableAlias = aTableName;
        } else {
            tableAlias = m_tables.getTableAliasByColumn(aColumnName);
        }
        tbl = m_tables.getTable(tableAlias);
        if (tbl == null) {
            return null;
        }
        realTableName = tbl.getName();
        Column col = tbl.getColumnByName(aColumnName);
        Type colType = col.getType();
        return new Semantino(colType,
                            m_factory.makeColumnRef(realTableName,
                                                    tableAlias,
                                                    aColumnName));
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
    public String printSemantinos() {
        String out = "Semantinos: ";
        while(!m_semantinoStack.isEmpty()) {
                Semantino next = m_semantinoStack.pop();
                out += "["+next.toString()+"]";
        }
        return out;
    }

    @Override
    public boolean hasSemantinos() {
        return !m_semantinoStack.isEmpty();
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
        assert(false);
        return null;
    }

}
