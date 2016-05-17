/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.importer.formatter.builtin;

import org.voltdb.importer.formatter.AbstractFormatterFactory;
import org.voltdb.importer.formatter.Formatter;

public class VoltCSVFormatterFactory extends AbstractFormatterFactory {

    public static final String[]  SUPER_CVS_PROPS = {"surrounding.spaces.need.quotes","nowhitespace","blank",
                "custom.null.string"};

    @Override
    public Formatter<String> create() {
        if(useSuperCsv()){
            return new VoltSuperCSVFormatter(m_formatName, m_formatProps);
        }
        return  new VoltCSVFormatter(m_formatName, m_formatProps);
    }

    private boolean useSuperCsv(){

        for(String prop : SUPER_CVS_PROPS){
            if(m_formatProps.containsKey(prop)) return true;
        }

        return false;
    }
}
