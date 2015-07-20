package org.voltdb.sqlparser.syntax;

/**
 * Hold the parts needed to define a column in an insert statement.
 *
 * @author bwhite
 */
public class ColumnIdent {
    private final String m_colName;
    private final int    m_colLineNo;
    private final int    m_colColNo;
    ColumnIdent(String aColName, int aColLineNo, int aColColNo) {
        m_colName = aColName;
        m_colLineNo = aColLineNo;
        m_colColNo = aColColNo;
    }
    public final String getColName() {
        return m_colName;
    }
    public final int getColLineNo() {
        return m_colLineNo;
    }
    public final int getColColNo() {
        return m_colColNo;
    }
}

