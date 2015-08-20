package org.voltdb.sqlparser.semantics.symtab;

import org.voltdb.sqlparser.syntax.symtab.TypeKind;

public class VoidType extends Type {
	public VoidType(String aName, TypeKind aKind) {
		super(aName, aKind);
	}
	
	@Override
	public boolean isVoidType() {
		return true;
	}
}
