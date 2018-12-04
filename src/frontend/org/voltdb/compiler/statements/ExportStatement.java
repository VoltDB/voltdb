package org.voltdb.compiler.statements;

import java.util.regex.Matcher;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

public class ExportStatement extends StatementProcessor {

    public ExportStatement(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    private static boolean isRegularTable(VoltXMLElement m_schema, String name) {
        for (VoltXMLElement element : m_schema.children) {
            if (element.name.equals("table")
                    && (!element.attributes.containsKey("export"))
                    && element.attributes.get("name").equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        Matcher statementMatcher = SQLParser.matchExportTable(ddlStatement.statement);
        if (!statementMatcher.matches()) {
            return false;
        }
        String tableName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement);
        String targetName = checkIdentifierStart(statementMatcher.group(2), ddlStatement.statement);

        if (!isRegularTable(m_schema, tableName)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid EXPORT TABLE statement: %s is not a table.",
                    tableName));
        }
        //System.out.println("XXX Exporting table " + tableName + ", to target " + targetName);
        //m_returnAfterThis = true;
        return true;
    }

}
