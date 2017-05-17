package org.voltdb.compiler;

public interface DDLParserCallback {
    public void statement(String statement, int lineNum) throws VoltCompiler.VoltCompilerException;
    public void batch(String batch, int batchEndLineNum) throws VoltCompiler.VoltCompilerException;
}
