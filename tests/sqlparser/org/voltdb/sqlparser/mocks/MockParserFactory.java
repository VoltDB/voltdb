/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package org.voltdb.sqlparser.mocks;

import java.util.List;

import org.voltdb.sqlparser.semantics.grammar.InsertStatement;
import org.voltdb.sqlparser.semantics.symtab.Semantino;
import org.voltdb.sqlparser.semantics.symtab.ParserFactory;
import org.voltdb.sqlparser.syntax.grammar.ICatalogAdapter;
import org.voltdb.sqlparser.syntax.grammar.ISemantino;
import org.voltdb.sqlparser.syntax.grammar.IOperator;
import org.voltdb.sqlparser.syntax.grammar.Projection;
import org.voltdb.sqlparser.syntax.symtab.IAST;
import org.voltdb.sqlparser.syntax.symtab.IParserFactory;
import org.voltdb.sqlparser.syntax.symtab.ISymbolTable;
import org.voltdb.sqlparser.syntax.symtab.IType;

public class MockParserFactory extends ParserFactory implements
        IParserFactory {

    public MockParserFactory(ICatalogAdapter aCatalog) {
        super(aCatalog);
        // TODO Auto-generated constructor stub
    }
    /*
     * All of these need to be implemented in VoltDB.  We define
     * them here instead of making them abstract to help with testing.
     * This makes it possible to test the parser and static semantics
     * checker without the rest of VoltDB and without mocking these
     * methods.
     */
    @Override
    public IAST makeQueryAST(List<Projection> aProjections,
                              IAST aWhereCondition,
                              ISymbolTable aTables) {
        unimplementedOperation("makeQueryAST");
        return null;
    }

    @Override
    public IAST makeInsertAST(InsertStatement aInsertStatement) {
        unimplementedOperation("makeInsertAST");
        return null;
    }

    @Override
    public IAST makeUnaryAST(IType t, boolean b) {
        unimplementedOperation("makeUnaryAST(boolean)");
        return null;
    }

    @Override
    public IAST makeUnaryAST(IType aIntType, int aValueOf) {
        unimplementedOperation("makeUnaryAST(int)");
        return null;
    }

    @Override
    public IAST makeBinaryAST(IOperator aOp,
                              ISemantino aLeftoperand,
                              ISemantino aRightoperand) {
        unimplementedOperation("makeBinaryAST");
        return null;
    }

    private void unimplementedOperation(String aFuncName) {
        throw new AssertionError("Unimplemented ParserFactory Method " + aFuncName);
    }

    @Override
    public IAST addTypeConversion(IAST aNode, IType aSrcType, IType aTrgType) {
        unimplementedOperation("addTypeConversion");
        return null;
    }

    @Override
    public IAST makeColumnRef(String aRealTableName,
                              String aTableAlias,
                              String aColumnName) {
        unimplementedOperation("makeColumnRef");
        return null;
    }

    public void processWhereExpression(Semantino aWhereExpression) {
            unimplementedOperation("processWhereExpression");
    }

}
