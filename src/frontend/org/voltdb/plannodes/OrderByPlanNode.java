/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.plannodes;

import java.util.*;
import org.json.JSONException;
import org.json.JSONStringer;
import org.voltdb.planner.PlannerContext;
import org.voltdb.types.*;

/**
 *
 */
public class OrderByPlanNode extends AbstractPlanNode {

    public enum Members {
        SORT_COLUMNS,
        COLUMN_NAME,
        COLUMN_GUID,
        SORT_DIRECTION;
    }

    /**
     * Sort Columns Indexes
     * The column index in the table that we should sort on
     */
    protected List<Integer> m_sortColumns = new Vector<Integer>();
    protected List<Integer> m_sortColumnGuids = new Vector<Integer>();
    protected List<String> m_sortColumnNames = new Vector<String>();
    /**
     * Sort Directions
     */
    protected List<SortDirectionType> m_sortDirections = new Vector<SortDirectionType>();

    /**
     * @param id
     */
    public OrderByPlanNode(PlannerContext context, Integer id) {
        super(context, id);
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.ORDERBY;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Make sure that they have the same # of columns and directions
        if (m_sortColumns.size() != m_sortDirections.size()) {
            throw new Exception("ERROR: PlanNode '" + toString() + "' has " +
                                "'" + m_sortColumns.size() + "' sort columns but " +
                                "'" + m_sortDirections.size() + "' sort directions");
        }

        // Make sure that none of the items are null
        for (int ctr = 0, cnt = m_sortColumns.size(); ctr < cnt; ctr++) {
            if (m_sortColumns.get(ctr) == null) {
                throw new Exception("ERROR: PlanNode '" + toString() + "' has a null " +
                                    "sort column index at position " + ctr);
            } else if (m_sortDirections.get(ctr) == null) {
                throw new Exception("ERROR: PlanNode '" + toString() + "' has a null " +
                                    "sort direction at position " + ctr);
            }
        }
    }

    /**
     * @return the sort_columns
     */
    public List<Integer> getSortColumns() {
        return m_sortColumns;
    }
    /**
     * @param sort_columns the sort_columns to set
     */
    public void setSortColumns(List<Integer> sort_columns) {
        m_sortColumns = sort_columns;
    }

    /**
     * @return the sort_column_guids
     */
    public List<Integer> getSortColumnGuids() {
        return m_sortColumnGuids;
    }

    /**
     * @return the sort_column_names
     */
    public List<String> getSortColumnNames() {
        return m_sortColumnNames;
    }
    /**
     * @param sort_column_names the sort_column_names to set
     */
    public void setSortColumnNames(List<String> sort_column_names) {
        m_sortColumnNames = sort_column_names;
    }

    /**
     * @return the sort_directions
     */
    public List<SortDirectionType> getSortDirections() {
        return m_sortDirections;
    }
    /**
     * @param sort_direction the sort_direction to set
     */
    public void setSortDirections(List<SortDirectionType> sort_direction) {
        m_sortDirections = sort_direction;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        assert (m_sortColumnNames.size() == m_sortDirections.size());
        stringer.key(Members.SORT_COLUMNS.name()).array();
        for (int ii = 0; ii < m_sortColumnNames.size(); ii++) {
            stringer.object();
            stringer.key(Members.COLUMN_NAME.name()).value(m_sortColumnNames.get(ii));
            stringer.key(Members.COLUMN_GUID.name()).value(m_sortColumnGuids.get(ii));
            stringer.key(Members.SORT_DIRECTION.name()).value(m_sortDirections.get(ii).toString());
            stringer.endObject();
        }
        stringer.endArray();
    }
}
