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
 package org.voltdb.sqlparser.syntax;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.voltdb.sqlparser.syntax.grammar.SQLParserLexer;
import org.voltdb.sqlparser.syntax.grammar.SQLParserParser;

/**
 * This class defines a driver for the parser.  We give it the
 * bits we need, the SQL, the error listener, and the kind of SQL
 * we expect to parse, and the driver does the rest.  The error
 * listener knows how to gather up error messages.
 *
 * The string is parsed in the constructor, to form an Antlr parse
 * tree.  The walk function below takes a listener object and walks
 * the tree, calling the appropriate listener functions as it goes.
 *
 * @author bwhite
 *
 */
public class SQLParserDriver {
    private ParseTree m_tree;
    /**
     * Construct an SQLParserDriver given a string.  Delegates to the InputStream
     * constructor.
     *
     * @param aDDL
     * @param aListener
     * @throws IOException
     */
    public SQLParserDriver(String aDDL, ANTLRErrorListener errorListener, SQLKind aKind) throws IOException {
        this(new ByteArrayInputStream(aDDL.getBytes(StandardCharsets.UTF_8)), errorListener, aKind);
    }

    /**
     * Construct an SQLParserDriver from a generic InputStream
     * and a listener.  The input stream is parsed during the
     * construction.  After the constructor's return, the
     * input string will *not* have been consumed.  Call SQLParserDriver.walk()
     * to fill the listener with happy static semantics.
     *
     * @param aInput
     * @param aListener
     * @throws IOException
     */
    public SQLParserDriver(InputStream aInput,
                           ANTLRErrorListener errorListener,
                           SQLKind aKind) throws IOException {
        ANTLRInputStream input = new ANTLRInputStream(aInput);

        // create a lexer that feeds off of input CharStream
        SQLParserLexer lexer = new SQLParserLexer(input);

        // create a buffer of tokens pulled from the lexer
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        // create a parser that feeds off the tokens buffer
        SQLParserParser parser = new SQLParserParser(tokens);
        // Set this to true to cause tracing.
        // parser.setTrace(true);

        if (errorListener != null) {
            parser.addErrorListener(errorListener);
        }
        switch (aKind) {
            case DML:
                m_tree = parser.data_manipulation_list();
                break;
            case DDL:
                m_tree = parser.data_definition_list();
                break;
            case DQL:
                m_tree = parser.data_query_list();
                break;
            default:
                throw new RuntimeException("Unknown SQL Kind asked for.");
        }
    }

    /**
     * Parse the input stream.
     */
    public void walk(VoltSQLVisitor aVisitor) {
        aVisitor.visit(m_tree);
    }
}
