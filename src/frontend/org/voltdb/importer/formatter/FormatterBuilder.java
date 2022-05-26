/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.lang.reflect.Constructor;
import java.util.Objects;
import java.util.Properties;

import org.voltdb.VoltDB;
import org.voltdb.importer.formatter.builtin.VoltCSVFormatterFactory;

/**
 * FormatterBuilder will delegate the formatter creation to concrete formatter factory to
 * instantiate formatter. Every import configuration has its own instance of FormatterBuilder which
 * will ensure that formatter is not shared among import configurations.
 */
public class FormatterBuilder {

    private final String m_formatterName;
    private final Properties m_formatterProps;
    private AbstractFormatterFactory m_formatterFactory;

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
    public Formatter create(){
        return m_formatterFactory.create(m_formatterName, m_formatterProps);
    }

    public void setFormatterFactory(AbstractFormatterFactory formatterFactory){
        m_formatterFactory = formatterFactory;
    }

    public String getFormatterName(){
        return m_formatterName;
    }

    public Properties getFormatterProperties()
    {
        return m_formatterProps;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_formatterName, m_formatterProps, m_formatterFactory);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof FormatterBuilder)) {
            return false;
        }
        FormatterBuilder other = (FormatterBuilder) o;
        return ((m_formatterName == null && other.m_formatterName == null) || m_formatterName.equalsIgnoreCase(other.m_formatterName))
            && ((m_formatterProps == null && other.m_formatterProps == null) || m_formatterProps.equals(other.m_formatterProps))
            && ((m_formatterFactory == null && other.m_formatterFactory == null) || m_formatterFactory.equals(other.m_formatterFactory));
    }

    /*
     * Create a FormatterBuilder from the supplied arguments. If no formatter is specified by configuration, return a default CSV formatter builder.
     */
    public static FormatterBuilder createFormatterBuilder(Properties formatterProperties) throws Exception {

        FormatterBuilder builder;
        AbstractFormatterFactory factory;

        if (formatterProperties.size() > 0) {
            String formatterClass = formatterProperties.getProperty("formatter");
            String format = formatterProperties.getProperty("format", "csv");
            Class<?> classz = Class.forName(formatterClass);
            Class<?>[] ctorParmTypes = new Class[]{ String.class, Properties.class };
            Constructor<?> ctor = classz.getDeclaredConstructor(ctorParmTypes);
            Object[] ctorParms = new Object[]{ format, formatterProperties };

            factory = new AbstractFormatterFactory() {
                @Override
                public Formatter create(String formatName, Properties props) {
                    try {
                        return (Formatter) ctor.newInstance(ctorParms);
                    }
                    catch (Exception e) {
                        VoltDB.crashLocalVoltDB("Failed to create formatter " + formatName);
                        return null;
                    }

                }
            };
            builder = new FormatterBuilder(format, formatterProperties);
        } else {
            factory = new VoltCSVFormatterFactory();
            Properties props = new Properties();
            factory.create("csv", props);
            builder = new FormatterBuilder("csv", props);
        }

        builder.setFormatterFactory(factory);
        return builder;
    }
}

