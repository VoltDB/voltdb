package org.voltdb.sqlparser.semantics.symtab;

public class ErrorSemantino extends Semantino {
	public ErrorSemantino() {
		super(SymbolTable.getErrorType(), null);
	}

	@Override
	public boolean isErrorSemantino() {
		return true;
	}
}
