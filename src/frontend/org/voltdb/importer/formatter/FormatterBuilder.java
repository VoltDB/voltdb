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

package org.voltdb.importer.formatter;

import java.util.Properties;

/**
 * FormatterBuilder will delegate the formatter creation to concrete formatter factory to
 * instantiate formatter. Every import configuration has its own instance of FormatterBuilder which
 * will ensure that formatter is not shared among import configurations.
 */
public class FormatterBuilder<T> {

    private final String m_formatterName;
    private final Properties m_formatterProps;
    private AbstractFormatterFactory  m_formatterFcatory;

    /**
     * Constructor
     * @param formatterName  The formatter name
     * @param formatterProps  The formatter properties from importer configuration
     */
    public FormatterBuilder(String formatterName, Properties formatterProps) {
        m_formatterName = formatterName;
        m_formatterProps = formatterProps;
    }

    /**
     * @return formatter instance created by its factory
     */
    public Formatter<?> create(){
        return m_formatterFcatory.create(m_formatterName, m_formatterProps);
    }

    public void setFormatterFactory(AbstractFormatterFactory  formatterFcatory){
        m_formatterFcatory = formatterFcatory;
    }

    public String getFormatterName(){
        return m_formatterName;
    }

    public Properties getFormatterProperties()
    {
        return m_formatterProps;
    }
}
