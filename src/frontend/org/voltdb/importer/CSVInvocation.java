/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.importer;

import au.com.bytecode.opencsv_voltpatches.CSVParser;
import java.io.IOException;
import org.voltdb.common.Constants;

/**
 *
 * @author akhanzode
 */
public class CSVInvocation implements Invocation {

    private final String m_line;
    private final String m_proc;
    private final CSVParser m_parser = new CSVParser();

    public CSVInvocation(String proc, String line) {
        m_line = line;
        m_proc = proc;
    }

    @Override
    public String getProcedure() {
        return m_proc;
    }

    @Override
    public Object[] getParams() throws IOException {
        Object list[] = m_parser.parseLine(m_line);
        if (list != null) {
            for (int i = 0; i < list.length; i++) {
                if ("NULL".equals(list[i])
                        || Constants.CSV_NULL.equals(list[i])
                        || Constants.QUOTED_CSV_NULL.equals(list[i])) {
                    list[i] = null;
                }
            }
        }
        return list;
    }

}
