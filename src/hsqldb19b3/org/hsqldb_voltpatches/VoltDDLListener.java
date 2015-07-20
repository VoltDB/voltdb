package org.hsqldb_voltpatches;

import org.voltdb.sqlparser.semantics.grammar.InsertStatement;
import org.voltdb.sqlparser.semantics.symtab.ParserFactory;
import org.voltdb.sqlparser.syntax.VoltSQLlistener;
import org.voltdb.sqlparser.syntax.grammar.IInsertStatement;
import org.voltdb.sqlparser.syntax.grammar.ISelectQuery;

/**
 * This is the listener for the Volt SQL Parser.  It really
 * needs to be in a different package.  However, that would
 * require moving VoltXMLElement, which is a bigger change
 * than we want to be making right now.
 *
 * @author bwhite
 *
 */
public class VoltDDLListener extends VoltSQLlistener {

    public VoltDDLListener(ParserFactory aFactory) {
        super(aFactory);
    }

    public VoltXMLElement getVoltXML() {
        IInsertStatement istat = getInsertStatement();
        if (istat != null) {
            return getVoltXML(istat);
        }
        ISelectQuery qstat = getSelectQuery();
        if (qstat != null) {
            return getVoltXML(qstat);
        }
        return null;
    }

    private VoltXMLElement getVoltXML(IInsertStatement aInsertStatement) {
        assert(aInsertStatement instanceof InsertStatement);
        InsertStatement insertStatement = (InsertStatement)aInsertStatement;
        VoltXMLElement top = new VoltXMLElement("insert");
        top.withValue("table", insertStatement.getTableName().toUpperCase());
        VoltXMLElement columns = new VoltXMLElement("columns");
        top.children.add(columns);
        for (int idx = 0; idx < insertStatement.getNumberColumns(); idx += 1) {
            VoltXMLElement col = new VoltXMLElement("column");
            columns.children.add(col);
            col.withValue("name", insertStatement.getColumnName(idx).toUpperCase());
            VoltXMLElement val = new VoltXMLElement("value");
            col.children.add(val);
            val.withValue("id", Integer.toString(idx+1));
            val.withValue("value", insertStatement.getColumnValue(idx));
            val.withValue("valuetype", insertStatement.getColumnType(idx).getName().toUpperCase());
        }
        VoltXMLElement params = new VoltXMLElement("parameters");
        top.children.add(params);
        return top;
    }

    private VoltXMLElement getVoltXML(ISelectQuery qstat) {
        return (VoltXMLElement)getFactory().makeQueryAST(qstat.getProjections(),
                                                         qstat.getWhereCondition(),
                                                         qstat.getTables());

    }


}
