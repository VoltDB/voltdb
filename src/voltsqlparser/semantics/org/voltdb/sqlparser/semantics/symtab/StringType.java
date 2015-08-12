package org.voltdb.sqlparser.semantics.symtab;

import org.voltdb.sqlparser.syntax.IStringType;
import org.voltdb.sqlparser.syntax.symtab.ITop;

public class StringType extends Type implements ITop, IStringType {
    long m_maxSize = -1;
    public StringType(String aName, TypeKind aKind) {
        super(aName, aKind);
    }
    private StringType(String aName, TypeKind aKind, long aMaxSize) {
        super(aName, aKind);
        m_maxSize = aMaxSize;
    }
    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.semantics.symtab.IStringType#makeInstance(long)
     */
    @Override
    public StringType makeInstance(long aSize) {
        StringType answer = new StringType(getName(), getTypeKind(), aSize);
        return answer;
    }
    public final long getMaxSize() {
        return m_maxSize;
    }
}
