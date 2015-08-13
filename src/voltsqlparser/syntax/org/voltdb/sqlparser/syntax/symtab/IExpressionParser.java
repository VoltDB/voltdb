package org.voltdb.sqlparser.syntax.symtab;

import org.voltdb.sqlparser.syntax.grammar.IOperator;
import org.voltdb.sqlparser.syntax.grammar.ISemantino;

public interface IExpressionParser {

    /**
     * The expression parser maintains a stack of semantinos.
     * This pops the top element from the stack.
     *
     * @return
     */
    ISemantino popSemantino();

    /**
     * The expression parser maintains a stack of semantinos.
     * This pushes a semantino onto the stack.
     *
     * @param aSemantino
     */
    void pushSemantino(ISemantino aSemantino);

    ISemantino getColumnSemantino(String aColumnName, String aTableName, ISymbolTable aTables);

    ISemantino getConstantSemantino(Object value, IType type);

    ISemantino getSemantinoMath(IOperator aOperator, ISemantino aLeftoperand,
            ISemantino aRightoperand);

    ISemantino getSemantinoCompare(IOperator aOperator, ISemantino aLeftoperand,
            ISemantino aRightoperand);

    ISemantino getSemantinoBoolean(IOperator aOperator, ISemantino aLeftoperand,
            ISemantino aRightoperand);


}
