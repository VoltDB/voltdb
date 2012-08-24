package org.voltdb.planner;

import java.util.ArrayList;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;

public class ParsedUnionStmt extends AbstractParsedStmt {

    public enum UnionType {
        NOUNION,
        UNION,
        UNION_ALL,
        INTERSECT,
        INTERSECT_ALL,
        EXCEPT_ALL,
        EXCEPT,
        UNION_TERM
    };
    
    public ArrayList<AbstractParsedStmt> m_children = new ArrayList<AbstractParsedStmt>();
    public UnionType m_unionType = UnionType.NOUNION;
    
    @Override
    void parse(VoltXMLElement stmtNode, Database db) {
        String type = stmtNode.attributes.get("uniontype");
        // Set operation type
        m_unionType = UnionType.valueOf(type);
        
        assert(stmtNode.children.size() == m_children.size());
        int i = 0;
        for (VoltXMLElement selectSQL : stmtNode.children) {
            AbstractParsedStmt nextSelectStmt = m_children.get(i++);
            nextSelectStmt.parse(selectSQL, db);
        }
    }
    
    /**Parse tables and parameters
     * .
     * @param root
     * @param db
     */
    void parseTablesAndParams(VoltXMLElement stmtNode, Database db) {
        
        assert(stmtNode.children.size() > 1);
        for (VoltXMLElement selectSQL : stmtNode.children) {
            if (!selectSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                throw new RuntimeException("Unexpected Element in UNION statement: " + selectSQL.name);
            }
            AbstractParsedStmt selectStmt = new ParsedSelectStmt();
            selectStmt.parseTablesAndParams(selectSQL, db);
            m_children.add(selectStmt);
            //tableList.addAll(selectStmt.tableList);
        }
        // MIKE. This is I don't understand why all fragments have the same table list
        // all tables across all selects in the union
        tableList.addAll(m_children.get(0).tableList);
    }

    /**Miscellaneous post parse activity
     * .
     * @param sql
     * @param db
     * @param joinOrder
     */
    void postParse(String sql, Database db, String joinOrder) {
        
        for (AbstractParsedStmt selectStmt : m_children) {
            selectStmt.postParse(sql, db, joinOrder);
        }
        // these just shouldn't happen right?
        assert(this.multiTableSelectionList.size() == 0);
        assert(this.noTableSelectionList.size() == 0);

        this.sql = sql;
        this.joinOrder = joinOrder;
    }


}
