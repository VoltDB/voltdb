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
 /**
 *
 */
package org.voltdb.sqlparser.semantics.symtab;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.sqlparser.semantics.grammar.InsertStatement;
import org.voltdb.sqlparser.semantics.grammar.SelectQuery;
import org.voltdb.sqlparser.syntax.IColumnIdent;
import org.voltdb.sqlparser.syntax.grammar.ICatalogAdapter;
import org.voltdb.sqlparser.syntax.grammar.IInsertStatement;
import org.voltdb.sqlparser.syntax.grammar.INeutrino;
import org.voltdb.sqlparser.syntax.grammar.IOperator;
import org.voltdb.sqlparser.syntax.grammar.ISelectQuery;
import org.voltdb.sqlparser.syntax.grammar.Projection;
import org.voltdb.sqlparser.syntax.symtab.IAST;
import org.voltdb.sqlparser.syntax.symtab.IColumn;
import org.voltdb.sqlparser.syntax.symtab.IParserFactory;
import org.voltdb.sqlparser.syntax.symtab.ISymbolTable;
import org.voltdb.sqlparser.syntax.symtab.ITable;
import org.voltdb.sqlparser.syntax.symtab.IType;
import org.voltdb.sqlparser.syntax.util.ErrorMessageSet;

/**
 * This is a generic implementation of the SQL Parser's
 * semantic operations.  It is not complete, but it will
 * be completed downstream.
 *
 * @author bwhite
 *
 */
public abstract class ParserFactory implements IParserFactory {
    ICatalogAdapter m_catalog;
    private static Map<String, IOperator> m_operatorMap = initOperatorMap();
    private Type m_booleanType = null;
    private static ISymbolTable m_stdPrelude = SymbolTable.newStandardPrelude();
    private ErrorMessageSet m_errorMessages = new ErrorMessageSet();

    private static Map<String, IOperator> initOperatorMap() {
        HashMap<String, IOperator> answer = new HashMap<String, IOperator>();
        for (Operator op : Operator.values()) {
            answer.put(op.getOperation(), op);
        }
        return answer;
    }

    public ParserFactory(ICatalogAdapter aCatalog) {
        m_catalog = aCatalog;
    }
    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.IParserFactory#getStandardPrelude()
     */
    @Override
    public ISymbolTable getStandardPrelude() {
        return m_stdPrelude;
    }

    @Override
    public IColumn newColumn(String aColName, IType aColType) {
        assert(aColType instanceof Type);
        return new Column(aColName, (Type)aColType);
    }

    @Override
    public ITable newTable(String aTableName) {
        return new Table(aTableName);
    }

    @Override
    public ICatalogAdapter getCatalog() {
        return m_catalog;
    }

    @Override
    public ISelectQuery newSelectQuery(ISymbolTable aSymbolTable,
                                       int aLineNo,
                                       int aColNo) {
        assert (aSymbolTable instanceof SymbolTable);
        SymbolTable symtab = (SymbolTable) aSymbolTable;
        return new SelectQuery(symtab,
                               this,
                               getErrorMessages(),
                               aLineNo,
                               aColNo);
    }

    /**
     * Process a query.
     */
    @Override
    public void processQuery(ISelectQuery aSelectQuery) {
        // put projections onto neutrino stack.
        aSelectQuery.setAST(makeQueryAST(aSelectQuery.getProjections(),
                                         aSelectQuery.getWhereCondition(),
                                         aSelectQuery.getTables()));
    }

    @Override
    public IInsertStatement newInsertStatement() {
        return new InsertStatement();
    }

    @Override
    public IOperator getExpressionOperator(String aText) {
        return m_operatorMap.get(aText);
    }

    private static Type hasSuperType(Type aLeftType, Type aRightType) {
        if (aLeftType.getTypeKind() == aRightType.getTypeKind()) {
            return aLeftType;
        }
        return null;
    }

    public Neutrino[] tuac(INeutrino ileft, INeutrino iright) {
        Neutrino left = (Neutrino)ileft;
        Neutrino right = (Neutrino)iright;
        Type leftType = left.getType();
        Type rightType = right.getType();
        if (leftType.isEqualType(rightType)) {
                return new Neutrino[]{left,right};
        } else {
            Type convertedType = hasSuperType(leftType, rightType);
            if (convertedType != null) {
                Neutrino lconverted = new Neutrino(convertedType,
                                                  addTypeConversion(left.getAST(),
                                                                    leftType,
                                                                    convertedType));
                Neutrino rconverted = new Neutrino(convertedType,
                                                   addTypeConversion(right.getAST(),
                                                                     rightType,
                                                                     convertedType));
                return new Neutrino[]{lconverted, rconverted};
            } else {
                m_errorMessages.addError(-1, -1, "Can't convert type \"%s\" to \"%s\"",
                                         leftType, rightType);
                return null;
            }
        }
    }

    @Override
    public ErrorMessageSet getErrorMessages() {
        return m_errorMessages;
    }

    @Override
    public IType getBooleanType() {
        return SymbolTable.getBooleanType();
    }

    @Override
    public IType getErrorType() {
        return SymbolTable.getErrorType();
    }

    @Override
    public IColumnIdent makeColumnRef(String aColName,
                                      int    aColLineNo,
                                      int    aColColNo) {
        return new ColumnIdent(aColName, aColLineNo, aColColNo);
    }

}
