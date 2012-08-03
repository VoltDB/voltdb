package org.voltdb.planner;

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
    
    public AbstractParsedStmt leftParcedStmt = null;
    public AbstractParsedStmt rightParcedStmt = null;
    public UnionType unionType = UnionType.NOUNION;
    
    @Override
    void parse(VoltXMLElement stmtNode, Database db) {
        String type = stmtNode.attributes.get("uniontype");
        // Set operation type
        unionType = UnionType.valueOf(type);
        
        assert(stmtNode.children.size() == 2);
        // parse left
        VoltXMLElement leftNode = stmtNode.children.get(0);
        leftParcedStmt = AbstractParsedStmt.parse(sql1, leftNode, db, joinOrder1);
        // parse right
        VoltXMLElement rightNode = stmtNode.children.get(1);
        leftParcedStmt = AbstractParsedStmt.parse(sql1, leftNode, db, joinOrder1);
    }

}
