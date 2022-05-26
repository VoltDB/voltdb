/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.e3.topics;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.voltdb.ParameterConverter;
import org.voltdb.e3.topics.TypedPropertiesBase.KeyBase;
import org.voltdb.utils.CompoundErrors;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Sets;

/**
 * Base class for Properties map where all property keys are predefined and have parsers and potentially validated
 *
 * @param <K> Type of key
 */
public abstract class TypedPropertiesBase<K extends KeyBase<?>> {
    private final Map<K, Object> m_properties;

    protected TypedPropertiesBase(Map<String, String> properties) {
        CompoundErrors errors = new CompoundErrors();
        Map<String, K> validKeys = getValidKeys();
        Set<String> unsupportedKeys = Sets.difference(properties.keySet(), validKeys.keySet());
        if (!unsupportedKeys.isEmpty()) {
            errors.addErrorMessage("Unsupported properties: " + unsupportedKeys);
        }

        ImmutableMap.Builder<K, Object> builder = ImmutableMap.builder();
        for (K property : validKeys.values()) {
            try {
                String strValue = properties.get(property.name());
                Object value = strValue == null ? property.getDefaultValue() : property.parseAndValidate(strValue);
                if (value != null) {
                    builder.put(property, value);
                }
            } catch (Exception e) {
                errors.addErrorMessage(e.getMessage());
            }
        }

        if (errors.hasErrors()) {
            throw new IllegalArgumentException(errors.getErrorMessage());
        }

        m_properties = builder.build();
    }

    /**
     * Retrieve the value associated with {@code key}
     * <p>
     * Note: Most implementations should override this method and make it public
     *
     * @param <T> Type of value associated with {@code key}
     * @param key instance
     * @return Value associated with key or {@code null}
     */
    @SuppressWarnings("unchecked")
    protected <T> T getProperty(K key) {
        return (T) m_properties.get(key);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TypedPropertiesBase)) {
            return false;
        }

        return m_properties.equals(((TypedPropertiesBase<?>) obj).m_properties);
    }

    @Override
    public String toString() {
        return m_properties.toString();
    }

    protected Map<K, Object> asMap() {
        return m_properties;
    }

    /**
     * @return A {@link Map} of all valid keys of {@link KeyBase#name()} to {@link KeyBase} instance
     */
    protected abstract Map<String, K> getValidKeys();

    /**
     * Base class for property keys
     *
     * @param <T> Type of value associated with this key
     */
    public static abstract class KeyBase<T> {
        private final String m_name;
        private final Class<T> m_class;
        private final boolean m_mutable;
        private final T m_default;
        private final Consumer<? super T> m_validator;

        protected static void mustBePositive(Number number) {
            if (number.doubleValue() <= 0) {
                throw new IllegalArgumentException("value must be positive: " + number);
            }
        }

        // Properties are mutable by default
        protected KeyBase(String name, Class<T> clazz, T defValue, Consumer<? super T> validator) {
            this(name, clazz, true, defValue, validator);
        }

        protected KeyBase(String name, Class<T> clazz, boolean mutable, T defValue, Consumer<? super T> validator) {
            m_name = name;
            m_class = clazz;
            m_mutable = mutable;
            m_default = defValue;
            m_validator = validator;
        }

        public String name() {
            return m_name;
        }

        public boolean isMutable() {
            return m_mutable;
        }

        public T getDefaultValue() {
            return m_default;
        }

        public T get(Map<String, String> properties) {
            String strValue = properties.get(m_name);
            return strValue == null ? m_default : parseAndValidate(strValue);
        }

        @Override
        public String toString() {
            return m_name;
        }

        protected Class<T> getValueClass() {
            return m_class;
        }

        @SuppressWarnings("unchecked")
        protected T parseValue(String strValue) {
            return (T) ParameterConverter.tryToMakeCompatible(m_class, strValue);
        }

        protected

        final T parseAndValidate(String strValue) {
            try {
                T value = parseValue(strValue);
                if (m_validator != null) {
                    m_validator.accept(value);
                }
                return value;
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid value for " + m_name + ": " + e.getMessage());
            }
        }
    }
}
