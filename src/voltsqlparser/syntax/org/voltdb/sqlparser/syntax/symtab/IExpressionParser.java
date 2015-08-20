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

    ISemantino getColumnSemantino(String aColumnName, String aTableName);

    ISemantino getConstantSemantino(Object value, IType type);

    ISemantino getSemantinoMath(IOperator aOperator, ISemantino aLeftoperand,
            ISemantino aRightoperand);

    ISemantino getSemantinoCompare(IOperator aOperator, ISemantino aLeftoperand,
            ISemantino aRightoperand);

    /**
     * Create a Semantino for a binary operation.  This would be OR or AND.  The
     * IOperator knows which one it is.
     * 
     * @param aOperator
     * @param aLeftoperand
     * @param aRightoperand
     * @return
     */
    ISemantino getSemantinoBoolean(IOperator aOperator, ISemantino aLeftoperand,
            ISemantino aRightoperand);

    /**
     * Create a Semantino for a unary boolean operation.  There is only one
     * of these, NOT, but we drag it along anyway.
     * 
     * @param op
     * @param operand
     * @return
     */
	ISemantino getSemantinoBoolean(IOperator op, ISemantino operand);


}
