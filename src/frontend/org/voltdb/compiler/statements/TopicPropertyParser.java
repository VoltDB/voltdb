/* This file is part of VoltDB.
 * Copyright (C) 2020 VoltDB Inc.
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

package org.voltdb.compiler.statements;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONTokener;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Property;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;

/**
 * A topic property parser used for CREATE TOPIC and ALTER TOPIC
 * Reusing a JSON parser since the property map looks a lot like JSON
 */
public class TopicPropertyParser {
    private final VoltCompiler m_compiler;
    private final String m_propertiesString;
    private final CatalogMap<Property> m_properties;

    public enum Mode {
        ADD,
        DROP,
        ALTER
    }
    private final Mode m_mode;

    TopicPropertyParser(VoltCompiler ddlCompiler, String propertiesString, CatalogMap<Property> properties) {
        this(ddlCompiler, propertiesString, properties, Mode.ADD);
    }

    TopicPropertyParser(VoltCompiler ddlCompiler, String propertiesString, CatalogMap<Property> properties,
            Mode mode) {
        m_compiler = ddlCompiler;
        m_propertiesString = propertiesString;
        m_properties = properties;
        m_mode = mode;
    }

    void parse() throws VoltCompilerException {
        // Reusing a JSON parser since the property map looks a lot like JSON
        JSONTokener tokener = new JSONTokener(m_propertiesString);
        boolean first = true;
        try {
            while (true) {
                if (!first) {
                    if (expectChar(tokener, ',', true)) {
                        return;
                    }
                }
                else {
                    first = false;
                }
                String key = getKeyOrValue(tokener);
                if (m_mode != Mode.DROP) {
                    expectChar(tokener, '=', false);
                }
                if (m_mode == Mode.ADD) {
                    m_properties.add(key).setValue(getKeyOrValue(tokener));
                }
                else if (m_mode == Mode.ALTER) {
                    String value = getKeyOrValue(tokener);
                    Property property = m_properties.get(key);
                    if (property == null) {
                        throw m_compiler.new VoltCompilerException("Unable to alter property: " + key + " (not found)");
                    }
                    else {
                        property.setValue(value);
                    }
                }
                else {
                    assert m_mode == Mode.DROP;
                    m_properties.delete(key);
                }
            }
        } catch (JSONException e) {
            throw m_compiler.new VoltCompilerException("Unable to parse properties: " + e.getMessage());
        }
    }

    private String getKeyOrValue(JSONTokener tokener) throws VoltCompilerException, JSONException {
        char c = tokener.nextClean();
        switch (c) {
            case ',':
            case '=':
                throw m_compiler.new VoltCompilerException("Unexpected token in properties: " + c + ": " + tokener);
            case 0:
                throw m_compiler.new VoltCompilerException("Unexpected end of properties: " + tokener);
            default:
                tokener.back();
                return tokener.nextValue().toString();
        }
    }

    private boolean expectChar(JSONTokener tokener, char expected, boolean endOk)
            throws JSONException, VoltCompilerException {
        char c = tokener.nextClean();
        if (c != expected) {
            if (c == 0) {
                if (endOk) {
                    return true;
                }
                throw m_compiler.new VoltCompilerException(
                        "Expected token '" + expected + "' in properties but encountered end of string: " + tokener);
            }
            throw m_compiler.new VoltCompilerException(
                    "Expected token '" + expected + "' in properties but encountered: '" + c + "': " + tokener);
        }
        return false;
    }
}
