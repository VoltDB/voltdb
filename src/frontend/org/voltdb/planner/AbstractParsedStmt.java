/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;

public abstract class AbstractParsedStmt {

    public static class TablePair {
        public Table t1;
        public Table t2;

        @Override
        public boolean equals(Object obj) {
            if ((obj instanceof TablePair) == false)
                return false;
            TablePair tp = (TablePair)obj;

            return (((t1 == tp.t1) && (t2 == tp.t2)) ||
                    ((t1 == tp.t2) && (t2 == tp.t1)));
        }

        @Override
        public int hashCode() {
            assert((t1.hashCode() ^ t2.hashCode()) == (t2.hashCode() ^ t1.hashCode()));

            return t1.hashCode() ^ t2.hashCode();
        }
    }


    public String sql;

    public VoltType[] paramList = new VoltType[0];

    protected HashMap<Long, Integer> m_paramsById = new HashMap<Long, Integer>();

    public ArrayList<Table> tableList = new ArrayList<Table>();

    public AbstractExpression where = null;

    public ArrayList<AbstractExpression> whereSelectionList = new ArrayList<AbstractExpression>();

    public ArrayList<AbstractExpression> noTableSelectionList = new ArrayList<AbstractExpression>();

    public ArrayList<AbstractExpression> multiTableSelectionList = new ArrayList<AbstractExpression>();

    public HashMap<Table, ArrayList<AbstractExpression>> tableFilterList = new HashMap<Table, ArrayList<AbstractExpression>>();

    public HashMap<TablePair, ArrayList<AbstractExpression>> joinSelectionList = new HashMap<TablePair, ArrayList<AbstractExpression>>();

    public HashMap<AbstractExpression, Set<AbstractExpression> > valueEquivalence = new HashMap<AbstractExpression, Set<AbstractExpression>>();

    //User specified join order, null if none is specified
    public String joinOrder = null;

    // Store a table-hashed list of the columns actually used by this statement.
    // XXX An unfortunately counter-intuitive (but hopefully temporary) meaning here:
    // if this is null, that means ALL the columns get used.
    public HashMap<String, ArrayList<SchemaColumn>> scanColumns = null;

    protected String[] m_paramValues;
    protected Database m_db;

    static final String INSERT_NODE_NAME = "insert";
    static final String UPDATE_NODE_NAME = "update";
    static final String DELETE_NODE_NAME = "delete";
    static final String SELECT_NODE_NAME = "select";
    static final String UNION_NODE_NAME  = "union";

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    protected AbstractParsedStmt(String[] paramValues, Database db) {
        this.m_paramValues = paramValues;
        this.m_db = db;
    }
    /**
     *
     * @param sql
     * @param xmlSQL
     * @param db
     */
    public static AbstractParsedStmt parse(String sql, VoltXMLElement stmtTypeElement, String[] paramValues, Database db, String joinOrder) {

        AbstractParsedStmt retval = null;

        if (stmtTypeElement == null) {
            System.err.println("Unexpected error parsing hsql parsed stmt xml");
            throw new RuntimeException("Unexpected error parsing hsql parsed stmt xml");
        }

        // create non-abstract instances
        if (stmtTypeElement.name.equalsIgnoreCase(INSERT_NODE_NAME)) {
            retval = new ParsedInsertStmt(paramValues, db);
        }
        else if (stmtTypeElement.name.equalsIgnoreCase(UPDATE_NODE_NAME)) {
            retval = new ParsedUpdateStmt(paramValues, db);
        }
        else if (stmtTypeElement.name.equalsIgnoreCase(DELETE_NODE_NAME)) {
            retval = new ParsedDeleteStmt(paramValues, db);
        }
        else if (stmtTypeElement.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
            retval = new ParsedSelectStmt(paramValues, db);
        }
        else if (stmtTypeElement.name.equalsIgnoreCase(UNION_NODE_NAME)) {
            retval = new ParsedUnionStmt(paramValues, db);
        }
        else {
            throw new RuntimeException("Unexpected Element: " + stmtTypeElement.name);
        }

        // parse tables and parameters
        retval.parseTablesAndParams(stmtTypeElement);

        // parse specifics
        retval.parse(stmtTypeElement);

        // post parse action
        retval.postParse(sql, joinOrder);

        return retval;
    }

    /**
     *
     * @param stmtElement
     * @param db
     */
    abstract void parse(VoltXMLElement stmtElement);

    /**Parse tables and parameters
     * .
     * @param root
     * @param db
     */
    void parseTablesAndParams(VoltXMLElement root) {
        for (VoltXMLElement node : root.children) {
            if (node.name.equalsIgnoreCase("parameters")) {
                this.parseParameters(node);
            }
            if (node.name.equalsIgnoreCase("tablescans")) {
                String str = node.toString();
                this.parseTables(node);
            }
            if (node.name.equalsIgnoreCase("scan_columns")) {
                this.parseScanColumns(node);
            }
        }
    }

    /**Miscellaneous post parse activity
     * .
     * @param sql
     * @param db
     * @param joinOrder
     */
    void postParse(String sql, String joinOrder) {
        // split up the where expression into categories
        this.analyzeWhereExpression();
        // these just shouldn't happen right?
        assert(this.multiTableSelectionList.size() == 0);
        assert(this.noTableSelectionList.size() == 0);

        this.sql = sql;
        this.joinOrder = joinOrder;
    }

    /**
     * Convert a HSQL VoltXML expression to an AbstractExpression tree.
     * @param root
     * @return configured AbstractExpression
     */
    AbstractExpression parseExpressionTree(VoltXMLElement root) {
        AbstractExpression exprTree = parseExpressionTree(m_paramsById, root);
        exprTree.resolveForDB(m_db);

        if (m_paramValues != null) {
            List<AbstractExpression> params = exprTree.findAllSubexpressionsOfClass(ParameterValueExpression.class);
            for (AbstractExpression ae : params) {
                ParameterValueExpression pve = (ParameterValueExpression) ae;
                ConstantValueExpression cve = pve.getOriginalValue();
                if (cve != null) {
                    cve.setValue(m_paramValues[pve.getParameterIndex()]);
                }
            }

        }
        return exprTree;
    }

    // TODO: This static function and the functions (below) that it calls to deal with various Expression types
    // are only marginally related to AbstractParsedStmt
    // -- the function is now also called by DDLCompiler with no AbstractParsedStmt in sight --
    // so, the methods COULD be relocated to class AbstractExpression or ExpressionUtil.
    static public AbstractExpression parseExpressionTree(HashMap<Long, Integer> paramsById, VoltXMLElement root) {
        String elementName = root.name.toLowerCase();
        AbstractExpression retval = null;

        if (elementName.equals("value")) {
            retval = parseValueExpression(paramsById, root);
        }
        else if (elementName.equals("columnref")) {
            retval = parseColumnRefExpression(root);
        }
        else if (elementName.equals("bool")) {
            retval = parseBooleanExpression(root);
        }
        else if (elementName.equals("operation")) {
            retval = parseOperationExpression(paramsById, root);
        }
        else if (elementName.equals("function")) {
            retval = parseFunctionExpression(paramsById, root);
        }
        else if (elementName.equals("asterisk")) {
            return null;
        }
        else
            throw new PlanningErrorException("Unsupported expression node '" + elementName + "'");

        return retval;
    }

    /**
     *
     * @param exprNode
     * @param attrs
     * @return
     */
    private static AbstractExpression parseValueExpression(HashMap<Long, Integer> paramsById, VoltXMLElement exprNode) {
        String type = exprNode.attributes.get("type");
        String isParam = exprNode.attributes.get("isparam");
        String isPlannerGenerated = exprNode.attributes.get("isplannergenerated");

        VoltType vt = VoltType.typeFromString(type);
        int size = VoltType.MAX_VALUE_LENGTH;
        assert(vt != VoltType.VOLTTABLE);

        if ((vt != VoltType.STRING) && (vt != VoltType.VARBINARY)) {
            if (vt == VoltType.NULL) size = 0;
            else size = vt.getLengthInBytesForFixedTypes();
        }
        // A ParameterValueExpression is needed to represent any user-provided or planner-injected parameter.
        boolean needParameter = (isParam != null) && (isParam.equalsIgnoreCase("true"));

        // A ConstantValueExpression is needed to represent a constant in the statement,
        // EVEN if that constant has been "parameterized" by the plan caching code.
        ConstantValueExpression cve = null;
        boolean needConstant = (needParameter == false) ||
            ((isPlannerGenerated != null) && (isPlannerGenerated.equalsIgnoreCase("true")));

        if (needConstant) {
            cve = new ConstantValueExpression();
            cve.setValueType(vt);
            cve.setValueSize(size);
            if ( ! needParameter && vt != VoltType.NULL) {
                String valueStr = exprNode.attributes.get("value");
                cve.setValue(valueStr);
            }
        }
        if (needParameter) {
            ParameterValueExpression expr = new ParameterValueExpression();
            long id = Long.parseLong(exprNode.attributes.get("id"));
            int paramIndex = paramIndexById(paramsById, id);

            expr.setValueType(vt);
            expr.setValueSize(size);
            expr.setParameterIndex(paramIndex);
            if (needConstant) {
                expr.setOriginalValue(cve);
            }
            return expr;
        }
        return cve;
    }

    /**
     *
     * @param exprNode
     * @param attrs
     * @return
     */
    private static AbstractExpression parseColumnRefExpression(VoltXMLElement exprNode) {
        TupleValueExpression expr = new TupleValueExpression();

        String alias = exprNode.attributes.get("alias");
        String tableName = exprNode.attributes.get("table");
        String columnName = exprNode.attributes.get("column");

        expr.setColumnAlias(alias);
        expr.setColumnName(columnName);
        expr.setTableName(tableName);

        return expr;
    }

    /**
     *
     * @param exprNode
     * @param attrs
     * @return
     */
    private static AbstractExpression parseBooleanExpression(VoltXMLElement exprNode) {
        ConstantValueExpression expr = new ConstantValueExpression();

        expr.setValueType(VoltType.BIGINT);
        expr.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
        if (exprNode.attributes.get("attrs").equalsIgnoreCase("true"))
            expr.setValue("1");
        else
            expr.setValue("0");
        return expr;
    }

    /**
     *
     * @param paramsById
     * @param exprNode
     * @param attrs
     * @return
     */
    private static AbstractExpression parseOperationExpression(HashMap<Long, Integer> paramsById, VoltXMLElement exprNode) {
        String type = exprNode.attributes.get("type");
        ExpressionType exprType = ExpressionType.get(type);
        AbstractExpression expr = null;

        if (exprType == ExpressionType.INVALID) {
            throw new PlanningErrorException("Unsupported operation type '" + type + "'");
        }
        try {
            expr = exprType.getExpressionClass().newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
        expr.setExpressionType(exprType);

        // If the operation type was 'simplecolumn' then it's going to turn
        // into a TVE and we need to bail out before we try parsing the
        // left and right subtrees
        if (expr instanceof TupleValueExpression)
        {
            return expr;
        }

        // Allow expressions to read expression-specific data from exprNode.
        // Looks like the design fully abstracts other volt classes from
        // the XML serialization?  Putting this here instead of in derived
        // Expression implementations.

        if (expr instanceof AggregateExpression) {
            String node;
            if ((node = exprNode.attributes.get("distinct")) != null) {
                AggregateExpression ae = (AggregateExpression)expr;
                ae.m_distinct = Boolean.parseBoolean(node);
            }
        }

        // get the first (left) node that is an element
        VoltXMLElement leftExprNode = exprNode.children.get(0);
        assert(leftExprNode != null);

        // get the second (right) node that is an element (might be null)
        VoltXMLElement rightExprNode = null;
        if (exprNode.children.size() > 1)
            rightExprNode = exprNode.children.get(1);

        // recursively parse the left subtree (could be another operator or
        // a constant/tuple/param value operand).
        AbstractExpression leftExpr = parseExpressionTree(paramsById, leftExprNode);
        assert((leftExpr != null) || (exprType == ExpressionType.AGGREGATE_COUNT));
        expr.setLeft(leftExpr);

        if (expr.needsRightExpression()) {
            assert(rightExprNode != null);

            // recursively parse the right subtree
            AbstractExpression rightExpr = parseExpressionTree(paramsById, rightExprNode);
            assert(rightExpr != null);
            expr.setRight(rightExpr);
        }

        return expr;
    }


    /**
     *
     * @param paramsById
     * @param exprNode
     * @return a new Function Expression
     */
    private static AbstractExpression parseFunctionExpression(HashMap<Long, Integer> paramsById, VoltXMLElement exprNode) {
        String name = exprNode.attributes.get("name").toLowerCase();
        String disabled = exprNode.attributes.get("disabled");
        if (disabled != null) {
            throw new PlanningErrorException("Function '" + name + "' is not supported in VoltDB: " + disabled);
        }
        String value_type_name = exprNode.attributes.get("type");
        VoltType value_type = VoltType.typeFromString(value_type_name);
        String id = exprNode.attributes.get("id");
        assert(id != null);
        int idArg = 0;
        try {
            idArg = Integer.parseInt(id);
        } catch (NumberFormatException nfe) {}
        assert(idArg > 0);
        String parameter = exprNode.attributes.get("parameter");
        String volt_alias = exprNode.attributes.get("volt_alias");
        if (volt_alias == null) {
            volt_alias = name; // volt shares the function name with HSQL
        }

        ArrayList<AbstractExpression> args = new ArrayList<AbstractExpression>();
        for (VoltXMLElement argNode : exprNode.children) {
            assert(argNode != null);
            // recursively parse each argument subtree (could be any kind of expression).
            AbstractExpression argExpr = parseExpressionTree(paramsById, argNode);
            assert(argExpr != null);
            args.add(argExpr);
        }

        FunctionExpression expr = new FunctionExpression();
        expr.setAttributes(name, volt_alias, idArg);
        expr.setArgs(args);
        if (value_type != null) {
            expr.setValueType(value_type);
            expr.setValueSize(value_type.getMaxLengthInBytes());
        }

        if (parameter != null) {
            int parameter_idx = -1; // invalid argument index
            try {
                parameter_idx = Integer.parseInt(parameter);
            } catch (NumberFormatException nfe) {}
            assert(parameter_idx >= 0); // better be valid by now.
            assert(parameter_idx < args.size()); // must refer to a provided argument
            expr.setParameterArg(parameter_idx);
        }

        return expr;
    }

    /**
     * Parse the scan_columns element out of the HSQL-generated XML.
     * Fills scanColumns with a list of the columns used in the plan, hashed by
     * table name.
     *
     * @param columnsNode
     */
    void parseScanColumns(VoltXMLElement columnsNode)
    {
        scanColumns = new HashMap<String, ArrayList<SchemaColumn>>();

        for (VoltXMLElement child : columnsNode.children) {
            assert(child.name.equals("columnref"));
            AbstractExpression col_exp = parseExpressionTree(child);
            // TupleValueExpressions are always specifically typed,
            // so there is no need for expression type specialization, here.
            assert(col_exp != null);
            assert(col_exp instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression)col_exp;
            SchemaColumn col = new SchemaColumn(tve.getTableName(),
                                                tve.getColumnName(),
                                                tve.getColumnAlias(),
                                                col_exp);
            ArrayList<SchemaColumn> table_cols = null;
            if (!scanColumns.containsKey(col.getTableName()))
            {
                table_cols = new ArrayList<SchemaColumn>();
                scanColumns.put(col.getTableName(), table_cols);
            }
            table_cols = scanColumns.get(col.getTableName());
            table_cols.add(col);
        }
    }

    /**
     *
     * @param tablesNode
     */
    private void parseTables(VoltXMLElement tablesNode) {
        Set<Table> visited = new HashSet<Table>(tableList);

        for (VoltXMLElement node : tablesNode.children) {
            if (node.name.equalsIgnoreCase("tablescan")) {

                String tableName = node.attributes.get("table");
                Table table = getTableFromDB(tableName);

                assert(table != null);

                if( visited.contains( table)) {
                    throw new PlanningErrorException("VoltDB does not yet support self joins, consider using views instead");
                }

                visited.add(table);
                tableList.add(table);
            }
        }
    }

    private void parseParameters(VoltXMLElement paramsNode) {
        paramList = new VoltType[paramsNode.children.size()];

        for (VoltXMLElement node : paramsNode.children) {
            if (node.name.equalsIgnoreCase("parameter")) {
                long id = Long.parseLong(node.attributes.get("id"));
                int index = Integer.parseInt(node.attributes.get("index"));
                String typeName = node.attributes.get("type");
                VoltType type = VoltType.typeFromString(typeName);
                m_paramsById.put(id, index);
                paramList[index] = type;
            }
        }
    }

    /**
     */
    void analyzeWhereExpression() {

        // nothing to do if there's no where expression
        if (where == null) return;

        // this first chunk of code breaks the code into a list of expression that
        // all have to be true for the where clause to be true

        ArrayDeque<AbstractExpression> in = new ArrayDeque<AbstractExpression>();
        ArrayDeque<AbstractExpression> out = new ArrayDeque<AbstractExpression>();
        in.add(where);

        AbstractExpression inExpr = null;
        while ((inExpr = in.poll()) != null) {
            if (inExpr.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                in.add(inExpr.getLeft());
                in.add(inExpr.getRight());
            }
            else {
                out.add(inExpr);
            }
        }

        // the where selection list contains all the clauses
        whereSelectionList.addAll(out);
        this.analyzeWhereExpression(whereSelectionList);
    }

    /**
     */
void analyzeWhereExpression(ArrayList<AbstractExpression> whereList) {
        // This next bit of code identifies which tables get classified how
        HashSet<Table> tableSet = new HashSet<Table>();
        for (AbstractExpression expr : whereList) {
            tableSet.clear();
            getTablesForExpression(expr, tableSet);
            if (tableSet.size() == 0) {
                noTableSelectionList.add(expr);
            }
            else if (tableSet.size() == 1) {
                Table table = (Table) tableSet.toArray()[0];

                ArrayList<AbstractExpression> exprs;
                if (tableFilterList.containsKey(table)) {
                    exprs = tableFilterList.get(table);
                }
                else {
                    exprs = new ArrayList<AbstractExpression>();
                    tableFilterList.put(table, exprs);
                }
                expr.m_isJoiningClause = false;
                addExprToEquivalenceSets(expr);
                exprs.add(expr);
            }
            else if (tableSet.size() == 2) {
                TablePair pair = new TablePair();
                pair.t1 = (Table) tableSet.toArray()[0];
                pair.t2 = (Table) tableSet.toArray()[1];

                ArrayList<AbstractExpression> exprs;
                if (joinSelectionList.containsKey(pair)) {
                    exprs = joinSelectionList.get(pair);
                }
                else {
                    exprs = new ArrayList<AbstractExpression>();
                    joinSelectionList.put(pair, exprs);
                }
                expr.m_isJoiningClause = true;
                addExprToEquivalenceSets(expr);
                exprs.add(expr);
            }
            else if (tableSet.size() > 2) {
                multiTableSelectionList.add(expr);
            }
        }
    }

    /**
     *
     * @param expr
     * @param tables
     */
    void getTablesForExpression(AbstractExpression expr, HashSet<Table> tables) {
        List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(expr);
        for (TupleValueExpression tupleExpr : tves) {
            String tableName = tupleExpr.getTableName();
            Table table = getTableFromDB(tableName);
            tables.add(table);
        }
    }

    protected Table getTableFromDB(String tableName) {
        Table table = m_db.getTables().getIgnoreCase(tableName);
        return table;
    }

    @Override
    public String toString() {
        String retval = "SQL:\n\t" + sql + "\n";

        retval += "PARAMETERS:\n\t";
        for (VoltType param : paramList) {
            retval += param.toString() + " ";
        }

        retval += "\nTABLE SOURCES:\n\t";
        for (Table table : tableList) {
            retval += table.getTypeName() + " ";
        }

        retval += "\nSCAN COLUMNS:\n";
        if (scanColumns != null)
        {
            for (String table : scanColumns.keySet())
            {
                retval += "\tTable: " + table + ":\n";
                for (SchemaColumn col : scanColumns.get(table))
                {
                    retval += "\t\tColumn: " + col.getColumnName() + ": ";
                    retval += col.getExpression().toString() + "\n";
                }
            }
        }
        else
        {
            retval += "\tALL\n";
        }

        if (where != null) {
            retval += "\nWHERE:\n";
            retval += "\t" + where.toString() + "\n";

            retval += "WHERE SELECTION LIST:\n";
            int i = 0;
            for (AbstractExpression expr : whereSelectionList)
                retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

            retval += "NO TABLE SELECTION LIST:\n";
            i = 0;
            for (AbstractExpression expr : noTableSelectionList)
                retval += "\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";

            retval += "TABLE FILTER LIST:\n";
            for (Entry<Table, ArrayList<AbstractExpression>> pair : tableFilterList.entrySet()) {
                i = 0;
                retval += "\tTABLE: " + pair.getKey().getTypeName() + "\n";
                for (AbstractExpression expr : pair.getValue())
                    retval += "\t\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";
            }

            retval += "JOIN CLAUSE LIST:\n";
            for (Entry<TablePair, ArrayList<AbstractExpression>> pair : joinSelectionList.entrySet()) {
                i = 0;
                retval += "\tTABLES: " + pair.getKey().t1.getTypeName() + " and " + pair.getKey().t2.getTypeName() + "\n";
                for (AbstractExpression expr : pair.getValue())
                    retval += "\t\t(" + String.valueOf(i++) + ") " + expr.toString() + "\n";
            }
        }
        return retval;
    }

    // TODO: This method COULD also get migrated with the parse...Expression functions
    // to class AbstractExpression or ExpressionUtil or possibly by itself to ParameterExpression
    protected static int paramIndexById(HashMap<Long, Integer> paramsById, long paramId) {
        if (paramId == -1) {
            return -1;
        }
        assert(paramsById.containsKey(paramId));
        return paramsById.get(paramId);
    }

    private void addExprToEquivalenceSets(AbstractExpression expr) {
        // Ignore expressions that are not of COMPARE_EQUAL type
        if (expr.getExpressionType() != ExpressionType.COMPARE_EQUAL) {
            return;
        }

        AbstractExpression leftExpr = expr.getLeft();
        AbstractExpression rightExpr = expr.getRight();
        // Can't use an expression based on a column value that is not just a simple column value.
        if ( ( ! (leftExpr instanceof TupleValueExpression)) && leftExpr.hasAnySubexpressionOfClass(TupleValueExpression.class) ) {
            return;
        }
        if ( ( ! (rightExpr instanceof TupleValueExpression)) && rightExpr.hasAnySubexpressionOfClass(TupleValueExpression.class) ) {
            return;
        }

        // Any two asserted-equal expressions need to map to the same equivalence set,
        // which must contain them and must be the only such set that contains them.
        Set<AbstractExpression> eqSet1 = null;
        if (valueEquivalence.containsKey(leftExpr)) {
            eqSet1 = valueEquivalence.get(leftExpr);
        }
        if (valueEquivalence.containsKey(rightExpr)) {
            Set<AbstractExpression> eqSet2 = valueEquivalence.get(rightExpr);
            if (eqSet1 == null) {
                // Add new leftExpr into existing rightExpr's eqSet.
                valueEquivalence.put(leftExpr, eqSet2);
                eqSet2.add(leftExpr);
            } else {
                // Merge eqSets, re-mapping all the rightExpr's equivalents into leftExpr's eqset.
                for (AbstractExpression eqMember : eqSet2) {
                    eqSet1.add(eqMember);
                    valueEquivalence.put(eqMember, eqSet1);
                }
            }
        } else {
            if (eqSet1 == null) {
                // Both leftExpr and rightExpr are new -- add leftExpr to the new eqSet first.
                eqSet1 = new HashSet<AbstractExpression>();
                valueEquivalence.put(leftExpr, eqSet1);
                eqSet1.add(leftExpr);
            }
            // Add new rightExpr into leftExpr's eqSet.
            valueEquivalence.put(rightExpr, eqSet1);
            eqSet1.add(rightExpr);
        }
    }

    /** Parse a where clause. This behavior is common to all kinds of statements.
     *  TODO: It's not clear why ParsedDeleteStmt has its own VERY SIMILAR code to do this in method parseCondition.
     *  There's a minor difference in how "ANDs" are modeled -- are they multiple condition nodes or
     *  single condition nodes with multiple children? That distinction may be due to an arbitrary difference
     *  in the parser's handling of different statements, but even if it's justified, this method could easily
     *  be extended to handle multiple multi-child conditionNodes.
     */
    protected void parseConditions(VoltXMLElement conditionNode) {
        if (conditionNode.children.size() == 0)
            return;

        VoltXMLElement exprNode = conditionNode.children.get(0);
        assert(where == null); // Should be non-reentrant -- not overwriting any previous value!
        where = parseExpressionTree(exprNode);
        assert(where != null);
        ExpressionUtil.finalizeValueTypes(where);
    }

}
