package org.voltdb.expressions;

import junit.framework.TestCase;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TinySexpSerializer extends TestCase {
    /**
     * Deserialize a Sexp to Abstract expression
     * @param arg Sexp
     * @return deserialized expression
     */
    public static AbstractExpression deserialize(String arg) {
        final String sexp = arg.trim();
        if (sexp.isEmpty()) {
            return null;
        } else {
            assertEquals("Unmatched parenthesis found: " + sexp, sexp.startsWith("("), sexp.endsWith(")"));
            if (sexp.startsWith("(! ") || sexp.startsWith("(@- ")) {    // unary operations: NOT and unary-minus
                final Pair<String, String> split = scan(sexp.substring(sexp.indexOf(' '), sexp.lastIndexOf(')')));
                return new OperatorExpression(sexp.startsWith("(! ") ?
                        ExpressionType.OPERATOR_NOT : ExpressionType.OPERATOR_UNARY_MINUS,
                        deserialize(split.getFirst()), null);
            } else if (sexp.startsWith("(+ ") || sexp.startsWith("(- ") || sexp.startsWith("(* ") || sexp.startsWith("(/ ")) {
                // arithmetic operations
                final Pair<String, String> split = scan(sexp.substring(sexp.indexOf(' '), sexp.lastIndexOf(')')));
                assertNotNull("Missing right argument inside arithmetic op: " + sexp, split.getSecond());
                ExpressionType type = ExpressionType.INVALID;
                switch(sexp.charAt(1)) {
                    case '+':
                        type = ExpressionType.OPERATOR_PLUS;
                        break;
                    case '-':
                        type = ExpressionType.OPERATOR_MINUS;
                        break;
                    case '*':
                        type = ExpressionType.OPERATOR_MULTIPLY;
                        break;
                    case '/':
                        type = ExpressionType.OPERATOR_DIVIDE;
                        break;
                    default:
                        fail("Programing error");
                }
                return new OperatorExpression(type, deserialize(split.getFirst()), deserialize(split.getSecond()));
            } else if (sexp.startsWith("(&& ") || sexp.startsWith("(|| ")) {
                final Pair<String, String> split = scan(sexp.substring(sexp.indexOf(' '), sexp.lastIndexOf(')')));
                assertNotNull("Missing right argument inside conjunction op: " + sexp, split.getSecond());
                return new ConjunctionExpression(sexp.startsWith("(&& ") ? ExpressionType.CONJUNCTION_AND : ExpressionType.CONJUNCTION_OR,
                        deserialize(split.getFirst()), deserialize(split.getSecond()));
            } if (sexp.startsWith("(= ") || sexp.startsWith("(!= ") || sexp.startsWith("(> ") || sexp.startsWith("(< ") ||
                    sexp.startsWith("(>= ") || sexp.startsWith("(<= ")) {
                final Pair<String, String> split = scan(sexp.substring(sexp.indexOf(' '), sexp.lastIndexOf(')')));
                assertNotNull("Missing right argument inside comparison op: " + sexp, split.getSecond());
                ExpressionType type = ExpressionType.INVALID;
                switch (sexp.charAt(1)) {
                    case '=':
                        type = ExpressionType.COMPARE_EQUAL;
                        break;
                    case '!':
                        type = ExpressionType.COMPARE_NOTEQUAL;
                        break;
                    case '>':
                        type = sexp.charAt(2) == '=' ? ExpressionType.COMPARE_GREATERTHANOREQUALTO :
                                ExpressionType.COMPARE_GREATERTHAN;
                        break;
                    case '<':
                        type = sexp.charAt(2) == '=' ? ExpressionType.COMPARE_LESSTHANOREQUALTO :
                                ExpressionType.COMPARE_LESSTHAN;
                        break;
                    default:
                        fail("Programing error");
                }
                return new ComparisonExpression(type, deserialize(split.getFirst()), deserialize(split.getSecond()));
            } else if (sexp.startsWith("(in ")) {
                final Pair<String, String> split = scan(sexp.substring(sexp.indexOf(' '), sexp.lastIndexOf(')')));
                final AbstractExpression right = deserialize(split.getSecond());
                assertTrue("Right term of (in a b) must be a VectorValueExpression. Got \"" +
                        split.getSecond() + "\"", right instanceof VectorValueExpression);
                return new InComparisonExpression(deserialize(split.getFirst()), right);
            } else if (sexp.startsWith("(fn")) {
                final Matcher m = Pattern.compile("^\\(fn-([a-zA-Z]+)-(\\d+)").matcher(sexp);
                assertTrue("Function usage: \"(fn-FunctionName-%d [arg1] ...)\"\n" +
                        "FunctionName is alphabetic, %d is arity of the function, non-negative.\n" +
                        "e.g. (fn-foo-0) (fn-bar-1 P0)\nGot " + sexp, m.find());
                final FunctionExpression expr = new FunctionExpression();
                expr.setAttributes(m.group(1), null, 0);
                expr.setArgs(getArgs(Integer.valueOf(m.group(2)), "Function call ", sexp));
                return expr;
            } else if (sexp.startsWith("(vec")) {
                final Matcher m = Pattern.compile("^\\(vec-(\\d+)").matcher(sexp);
                assertTrue("VectorValueExpression usage: \"(vec-%d [arg1] ...)\"\n" +
                        "%d is the length of the vector, non-negative.\n" +
                        "e.g. (vec-0) (vec-1 P0)\nGot " + sexp, m.find());
                final VectorValueExpression expr = new VectorValueExpression();
                expr.setArgs(getArgs(Integer.valueOf(m.group(1)), "VectorValueExpression ", sexp));
                return expr;
            } else if (sexp.startsWith("C")) {    // columnXXX ==> TupleValueExpression
                final int index = Integer.valueOf(sexp.substring(1, Math.max(sexp.indexOf(' '), sexp.length())));
                final TupleValueExpression e = new TupleValueExpression("foo"/* table name */,
                        sexp.substring(0, Math.max(sexp.indexOf(' '), sexp.length()))/* column name */,
                        index/* column index */);
                e.setValueType(VoltType.INTEGER);   // NOTE: assume all columns are of INT type
                return e;
            } else if (sexp.startsWith("P")) {      // PVE
                ParameterValueExpression expr = new ParameterValueExpression();
                if (sexp.charAt(1) != '?') {
                    expr.setOriginalValue(createCVE(sexp.substring(1, Math.max(sexp.indexOf(' '), sexp.length()))));
                }
                return expr;
            } else if (sexp.startsWith("c")) {  // CVE
                switch(sexp) {
                    case "ctrue":
                        return ConstantValueExpression.getTrue();
                    case "cfalse":
                        return ConstantValueExpression.getFalse();
                    default:
                        return createCVE(sexp.substring(1, Math.max(sexp.indexOf(' '), sexp.length())));
                }
            } else {
                fail("Error parsing at symbol \"" + sexp + "\"");
                return null;
            }
        }
    }

    /**
     * Serialize an abstract expression to Sexp
     * @param e expression to be serialized
     * @return serialized Sexp
     */
    public static String serialize(AbstractExpression e) {
        if (e == null) {
            return "";
        } else if (e instanceof ParameterValueExpression) {
            final ParameterValueExpression expr = (ParameterValueExpression) e;
            return "P" + (expr.getOriginalValue() == null ? "?" : expr.getOriginalValue().getValue());
        } else if (e instanceof ConstantValueExpression) {
            return "c" + ((ConstantValueExpression) e).getValue();
        } else if (e instanceof TupleValueExpression) {
            return ((TupleValueExpression) e).getColumnName();
        } else if (e instanceof InComparisonExpression) {
            return "(in " + serialize(e.getLeft()) + " " + serialize(e.getRight()) + ")";
        } else if (e instanceof ComparisonExpression) {
            String op = "";
            switch (e.getExpressionType()) {
                case COMPARE_EQUAL:
                    op = "="; break;
                case COMPARE_NOTEQUAL:
                    op = "!="; break;
                case COMPARE_LESSTHAN:
                    op = "<"; break;
                case COMPARE_LESSTHANOREQUALTO:
                    op = "<="; break;
                case COMPARE_GREATERTHAN:
                    op = ">"; break;
                case COMPARE_GREATERTHANOREQUALTO:
                    op = ">="; break;
                default:
                    fail("Unsupported comparison " + e.getExpressionType().toString());
            }
            return "(" + op + " " + serialize(e.getLeft()) + " " + serialize(e.getRight()) + ")";
        } else if (e instanceof OperatorExpression) {
            switch (e.getExpressionType()) {
                case OPERATOR_NOT:              // unary op
                    return "(! " + serialize(e.getLeft()) + ")";
                case OPERATOR_UNARY_MINUS:
                    return "(@- " + serialize(e.getLeft()) + ")";
                case OPERATOR_PLUS:                   // arithmetics
                    return "(+ " + serialize(e.getLeft()) + " " + serialize(e.getRight()) + ")";
                case OPERATOR_MINUS:
                    return "(- " + serialize(e.getLeft()) + " " + serialize(e.getRight()) + ")";
                case OPERATOR_MULTIPLY:
                    return "(* " + serialize(e.getLeft()) + " " + serialize(e.getRight()) + ")";
                case OPERATOR_DIVIDE:
                    return "(/ " + serialize(e.getLeft()) + " " + serialize(e.getRight()) + ")";
                default:
                    fail("Unsupported operator: " + e.getExpressionType().toString());
                    return "";
            }
        } else if (e instanceof ConjunctionExpression) {
            switch (e.getExpressionType()) {
                case CONJUNCTION_AND:           // binary logic
                    return "(&& " + serialize(e.getLeft()) + " " + serialize(e.getRight()) + ")";
                case CONJUNCTION_OR:
                    return "(|| " + serialize(e.getLeft()) + " " + serialize(e.getRight()) + ")";
                default:
                    fail("Unsupported conjunction operator: " + e.getExpressionType().toString());
                    return "";
            }
        } else if (e instanceof FunctionExpression) {
            final int numArgs = e.getArgs() == null ? 0 : e.getArgs().size();
            final FunctionExpression expr = (FunctionExpression) e;
            return "(fn-" + expr.getFunctionName() + "-" + Integer.toString(numArgs) +
                    (numArgs == 0 ? ")" : (" " +
                            expr.getArgs().stream().map(TinySexpSerializer::serialize).collect(Collectors.joining(" "))
                            + ")"));
        } else if (e instanceof VectorValueExpression) {
            final int numArgs = e.getArgs() == null ? 0 : e.getArgs().size();
            return "(vec-" + Integer.toString(numArgs) +
                    (numArgs == 0 ? ")" : (" " +
                            e.getArgs().stream().map(TinySexpSerializer::serialize).collect(Collectors.joining(" "))
                            + ")"));
        } else {
            fail("Unsupported operator: " + e.toString());
            return "";
        }
    }

    /**
     * Get arguments as space separated Sexps as a list of abstract expressions
     * @param arity number of arguments
     * @param errMsgPrefix prefix of error message in case of deserialization failure
     * @param sexp Sexp for those arguments to deserialize
     * @return a list of deserialized expressions
     */
    private static List<AbstractExpression> getArgs(int arity, String errMsgPrefix, String sexp) {
        List<AbstractExpression> args = null;
        if (arity > 0) {
            String funArgs = sexp.substring(sexp.indexOf(' '), sexp.lastIndexOf(')'));
            args = new ArrayList<>();
            for (int index = 0; index < arity; ++index) {
                Pair<String, String> rems = scan(funArgs);
                if (index + 1 < arity) {
                    assertTrue(errMsgPrefix + " expecting " + Integer.toString(arity) +
                                    " arguments, only got " + Integer.toString(index + 1) + ": sexp = \"" + sexp + "\"",
                            !rems.getSecond().isEmpty());
                } else {
                    assertTrue(errMsgPrefix + " expecting " + Integer.toString(arity) +
                            " arguments, got more: sexp = \"" + sexp + "\"", rems.getSecond().isEmpty());
                }
                args.add(deserialize(rems.getFirst()));
                funArgs = rems.getSecond();
            }
        }
        return args;
    }

    /**
     * Scan forward for one Sexp
     * @param src remaining text to scan
     * @return a pair of scanned Sexp and remaining text
     */
    private static Pair<String, String> scan(String src) {
        final String sexp = src.trim();
        if (sexp.charAt(0) == '(') {
            for (int count = 1, index = 1; index < sexp.length(); ++index) {
                if (sexp.charAt(index) == '(')
                    ++count;
                else if (sexp.charAt(index) == ')') {
                    if (--count == 0)
                        return new Pair<>(sexp.substring(0, index + 1), sexp.substring(index + 1).trim());
                }
            }
            fail("Unmatched parenthesis found: " + src);
            return new Pair<>("", "");
        } else {    // terminal
            final int startpos = sexp.indexOf(' ');
            if (startpos > 0) {
                return new Pair<>(sexp.substring(0, startpos), sexp.substring(startpos));
            } else {
                return new Pair<>(sexp, "");
            }
        }
    }

    /**
     * Create a CVE number constant. Integer type is preferred over Float type.
     * @param s string representing number
     * @return created CVE number literal
     */
    private static ConstantValueExpression createCVE(String s) {
        try {
            return new ConstantValueExpression(Integer.valueOf(s));
        } catch (NumberFormatException e) {
            return new ConstantValueExpression(Float.valueOf(s));
        }
    }

    public static void tester() {        // test TinySexpSerializer
        AbstractExpression e = deserialize("(= c1 c10)");
        assertEquals(ExpressionType.COMPARE_EQUAL, e.getExpressionType());
        assertTrue(e.getLeft() instanceof ConstantValueExpression &&
                e.getRight() instanceof ConstantValueExpression);
        assertEquals(((ConstantValueExpression) e.getLeft()).getValue(), "1");
        assertEquals(((ConstantValueExpression) e.getRight()).getValue(), "10");

        assertEquals(((ConstantValueExpression) deserialize("(!= c1.0 c1)").getLeft()).getValue(), "1.0");

        e = deserialize(" (> C5 P2) ");
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, e.getExpressionType());
        assertTrue(e.getLeft() instanceof TupleValueExpression && e.getRight() instanceof ParameterValueExpression);
        assertEquals(((TupleValueExpression)e.getLeft()).getColumnName(), "C5");
        assertEquals(((ParameterValueExpression) e.getRight()).getOriginalValue().getValue(), "2");

        e = deserialize("(@- (- P? P-2.2))");
        assertEquals(ExpressionType.OPERATOR_UNARY_MINUS, e.getExpressionType());
        e = e.getLeft();
        assertEquals(ExpressionType.OPERATOR_MINUS, e.getExpressionType());
        assertTrue(e.getLeft() instanceof ParameterValueExpression && e.getRight() instanceof ParameterValueExpression);
        assertNull(((ParameterValueExpression)e.getLeft()).getOriginalValue());
        assertEquals(((ParameterValueExpression) e.getRight()).getOriginalValue().getValue(), "-2.2");

        e = deserialize("(&& (+ c3 c4) (/ C5 C6))");
        assertEquals(ExpressionType.CONJUNCTION_AND, e.getExpressionType());
        AbstractExpression left = e.getLeft(), right = e.getRight();
        assertEquals(ExpressionType.OPERATOR_PLUS, left.getExpressionType());
        assertEquals(ExpressionType.OPERATOR_DIVIDE, right.getExpressionType());
        assertTrue(left.getLeft() instanceof ConstantValueExpression &&
                left.getRight() instanceof ConstantValueExpression);
        assertTrue(right.getLeft() instanceof TupleValueExpression &&
                right.getRight() instanceof TupleValueExpression);
        assertEquals(e, deserialize("(&& (+ c3    c4) (/ C5 C6   ) ) "));

        e = deserialize("(|| (! (&& C1 C2)) (!= (- C2 C4) C3))");
        assertEquals(ExpressionType.CONJUNCTION_OR, e.getExpressionType());
        left = e.getLeft();
        right = e.getRight();
        assertTrue(left instanceof OperatorExpression && right instanceof ComparisonExpression);
        assertEquals(ExpressionType.OPERATOR_NOT, left.getExpressionType());
        assertEquals(ExpressionType.COMPARE_NOTEQUAL, right.getExpressionType());
        left = left.getLeft();
        assertTrue(left instanceof ConjunctionExpression);
        right = right.getLeft();
        assertTrue(right instanceof OperatorExpression && right.getExpressionType().equals(ExpressionType.OPERATOR_MINUS));

        e = deserialize("(fn-foo-0)");
        assertTrue(e instanceof FunctionExpression);
        assertEquals(((FunctionExpression) e).getFunctionName(), "foo");
        assertNull(e.getArgs());
        assertEquals(1, deserialize("(fn-bar-1 P0)").getArgs().size());
        assertEquals(deserialize("c2"), deserialize("(fn-baz-2 P0 (< C1 c2))").getArgs().get(1).getRight());

        e = deserialize("(vec-0)");
        assertTrue(e instanceof VectorValueExpression && e.getArgs() == null);
        e = deserialize("(vec-1 C2)");
        assertEquals(e.getArgs().size(), 1);
        assertEquals(serialize(e.getArgs().get(0)), "C2");

        e = deserialize("(in C1 (vec-4 P0 P1 P2 P3))");
        assertTrue(e instanceof InComparisonExpression);
        assertEquals("C1", serialize(e.getLeft()));
        assertEquals("(vec-4 P0 P1 P2 P3)", serialize(e.getRight()));

        // test both directions
        new ArrayList<String>() {{
            add("(|| (! (&& C1 C2)) (!= (- C2 C4) C3))");
            add("(&& (> (+ c1 P2) (/ C1 C2)) (! (!= (* (@- C1) c3) (- P2.35 C4))))");
            add("(@- (fn-foo-3 C1 (> C2 P?) (* C1 c2)))");
            add("(vec-0)");
            add("(vec-4 C1 P2 c3 (* c0 c1))");
            add("(in C1 (vec-4 P0 P1 P2 P3))");
            add("(&& ctrue (! cfalse))");
        }}.forEach(expr -> assertEquals(expr, serialize(deserialize(expr))));
    }
}
