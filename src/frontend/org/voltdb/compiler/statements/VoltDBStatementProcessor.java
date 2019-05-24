/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.compiler.statements;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import com.google_voltpatches.common.cache.CacheBuilder;
import com.google_voltpatches.common.cache.CacheLoader;
import com.google_voltpatches.common.cache.LoadingCache;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Check if a statement is VoltDB-specific.
 * If it is, then hand it over to the subsequent processors to handle.
 * If not, end the chain now and give it to HSQL for handling.
 */
public class VoltDBStatementProcessor extends StatementProcessor {
    private static final LoadingCache<String, Optional<String>> PREAMBLE_STATEMENT_CACHE = CacheBuilder.newBuilder()
            .maximumSize(5_000)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build(CacheLoader.from(VoltDBStatementProcessor::matchAllVoltDBStatementPreambles));

    public VoltDBStatementProcessor(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    private String m_commandPrefix;

    public String getCommandPrefix() {
        return m_commandPrefix;
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        if (ddlStatement.statement == null || ddlStatement.statement.trim().isEmpty()) {
            m_returnAfterThis = true;
            return false;
        }
        ddlStatement.statement = ddlStatement.statement.trim();

        try {
            final Optional<String> commandPrefix = PREAMBLE_STATEMENT_CACHE.get(ddlStatement.statement);
            if (!commandPrefix.isPresent()) {
                m_returnAfterThis = true;
                return false;
            }
            // Either PROCEDURE, FUNCTION, REPLICATE, PARTITION, ROLE, EXPORT or DR
            m_commandPrefix = commandPrefix.get();
            return false;
        } catch (ExecutionException e) {
            // Our cache loader method never throws, so this *should* never happen. But we have to be
            // safe just in case. Unfortunately, we can't actually throw a VoltCompilerException here
            // because it's not a static class and we don't have a VoltCompiler instance with which to
            // instantiate the VoltCompilerException.
            throw new RuntimeException("Error parsing DDL via statement cache", e);
        }
    }

    private static Optional<String> matchAllVoltDBStatementPreambles(String statement) {
        final Matcher statementMatcher = SQLParser.matchAllVoltDBStatementPreambles(statement);
        if (!statementMatcher.find()) {
            return Optional.empty();
        }
        return Optional.of(statementMatcher.group(1).toUpperCase());
    }

}
