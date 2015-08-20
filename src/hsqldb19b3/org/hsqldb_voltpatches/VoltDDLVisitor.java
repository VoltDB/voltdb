package org.hsqldb_voltpatches;

import org.voltdb.sqlparser.semantics.grammar.InsertStatement;
import org.voltdb.sqlparser.semantics.symtab.ParserFactory;
import org.voltdb.sqlparser.syntax.VoltSQLVisitor;
import org.voltdb.sqlparser.syntax.grammar.IInsertStatement;
import org.voltdb.sqlparser.syntax.grammar.ISelectQuery;

public class VoltDDLVisitor extends VoltSQLVisitor {
    public VoltDDLVisitor(ParserFactory aFactory) {
        super(aFactory);
    }

    /**
     * Figure out what kind of statement this is and
     * calculate the VoltXMLElement for it.  This needs to be moved
     * someplace else, probably to the factory object.
     *
     * @return
     */
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

    /**
     * Calculate the VoltXMLElement for an insert statement.  This
     * probably needs to be moved to the factory object.
     *
     * @param aInsertStatement
     * @return
     */
    private VoltXMLElement getVoltXML(IInsertStatement aInsertStatement) {
        assert(aInsertStatement instanceof InsertStatement);
        return (VoltXMLElement)getFactory().makeInsertAST((InsertStatement)aInsertStatement);
    }

    private VoltXMLElement getVoltXML(ISelectQuery qstat) {
        return (VoltXMLElement)getFactory().makeQueryAST(qstat.getProjections(),
                                                         qstat.getWhereCondition(),
                                                         qstat.getTables());

    }
}
