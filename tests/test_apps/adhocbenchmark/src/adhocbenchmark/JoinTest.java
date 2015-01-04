/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */


package adhocbenchmark;


/**
 * Configuration that determines what the generated join queries look like.
 */
class JoinTest extends QueryTestBase {
    // Number of join levels
    private final int m_nLevels;
    private final String m_filterColumn;
    private final boolean m_replicatedQuery;
    private final String m_joinColumn;

    private JoinTest(final String tablePrefix, final int nTables,
                     final String columnPrefix, final int nColumns, final int nLevels,
                     final String filterColumn, final boolean replicatedQuery, final String joinColumn) {
        super(tablePrefix, columnPrefix, nColumns, nTables);
        m_nLevels = nLevels;
        m_filterColumn = filterColumn;
        m_replicatedQuery = replicatedQuery;
        m_joinColumn = joinColumn;
    }

    @Override
    public String getQuery(int iQuery, QueryTestHelper helper) {
        // Generate table lists by grabbing n sequential numbers at a time (wrap around).
        int iStart = iQuery * m_nLevels;
        String parentTable;
        // In the SPChain configuration, replicated tables are used solely for the sake of (allowing) chaining
        // without having to partition the generated tables on PARENT_ID.
        // So, the parent table wants to explicitly be a partitioned table, not picked at random from the generated tables.
        // If we DID switch over to partitioning the generated tables on PARENT_ID, the parent table would still
        // need to be hard-coded because it would still want to be partitioned on ID, instead.
        if (m_replicatedQuery ||  m_joinColumn.equals("ID")) {
            // By default, choose the parent table randomly from those generated.
            parentTable = helper.tableName(helper.getShuffledNumber(iStart));
        } else {
            // Force a parent table partitioned by ID
            // -- at some risk, this name assumes that schema was also generated for a different
            // configuration of this test (that needed partitioned tables)
            parentTable = "joinPART_1";
        }
        StringBuilder query = new StringBuilder("SELECT * FROM " + parentTable + " T0");
        for (int i = 1; i < m_nLevels; i++) {
            query.append(", ");
            query.append(helper.tableName(helper.getShuffledNumber(iStart + i)) + " T" + i);
        }

        // For SP, the parent table has a where clause with random constant ID filter
        // on the assumption that ID is the partition key.
        // Other cases use the never-partitioning PARENT_ID.
        query.append(" WHERE T0." + m_filterColumn + " = " + helper.getShuffledNumber(0));

        // Each child table has a where clause with an equality join to a prior table.
        // In "star configuration", the join is a primary-to-primary join, (possibly on a partition key).
        // In "chain configuration", the join is a foreign-to-primary join,
        // (requiring the child tables to be replicated -- or IN THEORY, they could be foreign-key-partitioned.
        for (int i = 1; i < m_nLevels; i++) {
            query.append(" AND T" + i + "." + m_joinColumn + " = T" + (i-1) + ".ID");
        }
        return query.toString();
    }

    public static class ChainFactory implements QueryTestBase.Factory {
        @Override
        public QueryTestBase make(final String tablePrefix, int nVariations, final String columnPrefix,
                final int nColumns, final int nRandomNumbers) {
            return new JoinTest(tablePrefix, nVariations, columnPrefix, nColumns, nRandomNumbers, "PARENT_ID", true, "PARENT_ID");
        }
    }

    public static class SPChainFactory implements QueryTestBase.Factory {
        @Override
        public QueryTestBase make(final String tablePrefix, int nVariations, final String columnPrefix,
                final int nColumns, final int nRandomNumbers) {
            return new JoinTest(tablePrefix, nVariations, columnPrefix, nColumns, nRandomNumbers, "ID", false, "PARENT_ID");
        }
    }

    public static class MPChainFactory implements QueryTestBase.Factory {
        @Override
        public QueryTestBase make(final String tablePrefix, int nVariations, final String columnPrefix,
                final int nColumns, final int nRandomNumbers) {
            return new JoinTest(tablePrefix, nVariations, columnPrefix, nColumns, nRandomNumbers, "PARENT_ID", false, "PARENT_ID");
        }
    }

    public static class StarFactory implements QueryTestBase.Factory {
        @Override
        public QueryTestBase make(final String tablePrefix, int nVariations, final String columnPrefix,
                final int nColumns, final int nRandomNumbers) {
            return new JoinTest(tablePrefix, nVariations, columnPrefix, nColumns, nRandomNumbers, "PARENT_ID", true, "ID");
        }
    }

    public static class SPStarFactory implements QueryTestBase.Factory {
        @Override
        public QueryTestBase make(final String tablePrefix, int nVariations, final String columnPrefix,
                final int nColumns, final int nRandomNumbers) {
            return new JoinTest(tablePrefix, nVariations, columnPrefix, nColumns, nRandomNumbers, "ID", false, "ID");
        }
    }

    public static class MPStarFactory implements QueryTestBase.Factory {
        @Override
        public QueryTestBase make(final String tablePrefix, int nVariations, final String columnPrefix,
                final int nColumns, final int nRandomNumbers) {
            return new JoinTest(tablePrefix, nVariations, columnPrefix, nColumns, nRandomNumbers, "PARENT_ID", false, "ID");
        }
    }

}