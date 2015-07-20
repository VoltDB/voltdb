package org.hsqldb_voltpatches;

import java.util.List;

import org.voltdb.sqlparser.syntax.symtab.IAST;
import org.voltdb.sqlparser.syntax.symtab.IParserFactory;
import org.voltdb.sqlparser.semantics.symtab.ParserFactory;
import org.voltdb.sqlparser.semantics.symtab.SymbolTable;
import org.voltdb.sqlparser.semantics.symtab.Type;
import org.voltdb.sqlparser.syntax.grammar.ICatalogAdapter;
import org.voltdb.sqlparser.syntax.grammar.INeutrino;
import org.voltdb.sqlparser.syntax.grammar.IOperator;
import org.voltdb.sqlparser.syntax.grammar.Projection;
import org.voltdb.sqlparser.syntax.symtab.ISymbolTable;
import org.voltdb.sqlparser.syntax.symtab.IType;
import org.voltdb.sqlparser.syntax.util.ErrorMessageSet;

public class VoltParserFactory extends ParserFactory implements IParserFactory {
    int m_id = 1;
    public VoltParserFactory(ICatalogAdapter aCatalog) {
        super(aCatalog);
    }

    private String newId() {
        return Integer.toString(m_id++);
    }

    @Override
    public IAST makeUnaryAST(IType aIntType, int aValueOf) {
        Type intType = (Type)aIntType;
        VoltXMLElement answer = new VoltXMLElement("value");
        answer.withValue("id", newId());
        answer.withValue("value", Integer.toString(aValueOf));
        answer.withValue("valuetype", intType.getName().toUpperCase());
        return answer;
    }

    @Override
    public IAST makeUnaryAST(IType aIntType, boolean aValueOf) {
        Type intType = (Type)aIntType;
        VoltXMLElement answer = new VoltXMLElement("value");
        answer.withValue("id", newId());
        answer.withValue("value", Boolean.toString(aValueOf));
        answer.withValue("valuetype", intType.getName().toUpperCase());
        return answer;
    }

    @Override
    public IAST makeBinaryAST(IOperator aOp,
                              INeutrino aLeftoperand,
                              INeutrino aRightoperand) {
        VoltXMLElement answer = new VoltXMLElement("operation");
        answer.withValue("id", newId())
              .withValue("optype", aOp.getVoltOperation());
        answer.children.add((VoltXMLElement)aLeftoperand.getAST());
        answer.children.add((VoltXMLElement)aRightoperand.getAST());
        return answer;
    }

    private void unimplementedOperation(String aFuncName) {
        throw new AssertionError("Unimplemented ParserFactory Method " + aFuncName);
    }

    @Override
    public IAST addTypeConversion(IAST aNode, IType aSrcType, IType aTrgType) {
        unimplementedOperation("addTypeConversion");
        return null;
    }

    @Override
    public IAST makeQueryAST(List<Projection> aProjections,
                              IAST aWhereCondition,
                              ISymbolTable aTables) {
        VoltXMLElement columns = makeColumns(aProjections, aTables);
        VoltXMLElement params = makeParams();
        VoltXMLElement joincond = makeJoinCond(aWhereCondition);
        VoltXMLElement tablescans = makeTableScans(joincond, (SymbolTable)aTables);
        VoltXMLElement answer = new VoltXMLElement("select");
        answer.children.add(columns);
        answer.children.add(params);
        answer.children.add(tablescans);
        return answer;
    }

    private VoltXMLElement makeParams() {
        VoltXMLElement answer = new VoltXMLElement("parameters");
        return answer;
    }

    private VoltXMLElement makeJoinCond(IAST aWhereCondition) {
        if (aWhereCondition != null) {
            VoltXMLElement answer = new VoltXMLElement("joincond");
            answer.children.add((VoltXMLElement)aWhereCondition);
            return answer;
        }
        return null;
    }

    private VoltXMLElement makeTableScans(VoltXMLElement aJoincond, SymbolTable aTables) {
        VoltXMLElement answer = new VoltXMLElement("tablescans");
        VoltXMLElement lastChild = null;
        for (SymbolTable.TablePair tp : aTables.getTables()) {
            VoltXMLElement scan = new VoltXMLElement("tablescan");
            scan.withValue("jointype", "inner")
                .withValue("table", tp.getTable().getName().toUpperCase())
                .withValue("tablealias", tp.getAlias().toUpperCase());
            answer.children.add(scan);
            lastChild = scan;
        }
        if (aJoincond != null) {
            lastChild.children.add(aJoincond);
        }
        return answer;
    }

    private VoltXMLElement makeColumns(List<Projection> aProjections,
                                       ISymbolTable aTables) {
        SymbolTable symtab = (SymbolTable)aTables;
        VoltXMLElement answer = new VoltXMLElement("columns");
        for (Projection proj : aProjections) {
            VoltXMLElement colref = new VoltXMLElement("columnref");
            answer.children.add(colref);
            String colName = proj.getColumnName();
            String tableName = proj.getTableName();
            if (tableName == null) {
                tableName = symtab.getTableNameByColumn(colName);
            }
            if (tableName == null) {
                getErrorMessages().addError(proj.getLineNo(),
                                            proj.getColNo(),
                                            "Cannot find column named \"%s\"",
                                            colName);
                tableName = "<<NOT_FOUND>>";
            }
            String alias = proj.getAlias();
            if (alias == null) {
                alias = colName;
            }
            colref.withValue("id", newId())
                  .withValue("table", tableName.toUpperCase())
                  .withValue("column", colName.toUpperCase())
                  .withValue("alias", alias.toUpperCase());
        }
        return answer;
    }

    @Override
    public IAST makeColumnRef(String aRealTableName,
                              String aTableAlias,
                              String aColumnName) {
        VoltXMLElement answer = new VoltXMLElement("columnref");
        answer.withValue("alias", aColumnName.toUpperCase())
              .withValue("column", aColumnName.toUpperCase())
              .withValue("id", newId())
              .withValue("table", aRealTableName.toUpperCase())
              .withValue("tableAlias", aTableAlias.toUpperCase());
        return answer;
    }
}
