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

package org.voltdb.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Objects;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;

import com.google_voltpatches.common.primitives.Primitives;

/**
 * Utility class for loading and constructing pro classes which might not exist in community edition
 *
 * @param <T> Type of pro class
 */
public class ProClass<T> {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private static final ProClass<?> NO_CLASS = new ProClass<Object>(null) {
        @Override
        public ProClass<Object> useConstructorFor(Class<?>... parameterTypes) {
            return this;
        }

        @Override
        public Object newInstance(Object... parameters) {
            return null;
        }
    };;

    /**
     * {@link ErrorHandler} which just ignores and drops the error on the floor
     */
    public static final ErrorHandler HANDLER_IGNORE = (m, t) -> {};

    /**
     * {@link ErrorHandler} which logs only the error message in the host log at warning
     */
    public static final ErrorHandler HANDLER_LOG = (m, t) -> hostLog.warn(m);

    /**
     * {@link ErrorHandler} which calls {@link VoltDB#crashLocalVoltDB(String, boolean, Throwable)}
     */
    public static final ErrorHandler HANDLER_CRASH = (m, t) -> VoltDB.crashLocalVoltDB(m, true, t);

    private final Class<T> m_proClass;
    private ErrorHandler m_errorHandler = HANDLER_CRASH;
    private boolean m_loadConstructor = true;
    private Constructor<T> m_constructor;

    /**
     * Try to load a PRO class. If the load fails {@code loadFailure} will be invoked to handle the error.
     *
     * @param className   The class name of the PRO class
     * @param feature     The name of the feature
     * @param loadFailure {@link ErrorHandler} for handling when class loading fails
     * @return an instance of {@link ProClass}. Never {@code null}.
     */
    @SuppressWarnings("unchecked")
    public static <T> ProClass<T> load(String className, String feature, ErrorHandler loadFailure) {
        try {
            return new ProClass<T>((Class<T>) ProClass.class.getClassLoader().loadClass(className));
        } catch (ClassNotFoundException e) {
            loadFailure.handle("Cannot load pro" + className + " in VoltDB community edition to support the feature "
                    + feature + '.', e);
        }

        return (ProClass<T>) NO_CLASS;
    }

    /**
     * Construct a new instance of {@code className} using {@code parameters}. If the class cannot be loaded
     * {@code loadFailure} will be invoked and {@code null} is returned. If the class cannot be constructed
     * {@link VoltDB#crashLocalVoltDB(String, boolean, Throwable)} will be invoked with the instantiation exception.
     *
     * @param className   The class name of the PRO class
     * @param feature     The name of the feature
     * @param loadFailure {@link ErrorHandler} for handling when class loading fails
     * @param parameters  to pass to the constructor of {@code className}
     * @return an instance of {@code T}
     */
    public static <T> T newInstanceOf(String className, String feature, ErrorHandler loadFailure,
            Object... parameters) {
        return ProClass.<T>load(className, feature, loadFailure).newInstance(parameters);
    }

    private ProClass(Class<T> proClass) {
        super();
        m_proClass = proClass;
    }

    /**
     * @param errorHandler {@link ErrorHandler} to invoke when an exception occurs. Defaults to {@link #HANDLER_CRASH}
     * @return {@code this}
     */
    public ProClass<T> errorHandler(ErrorHandler errorHandler) {
        m_errorHandler = Objects.requireNonNull(errorHandler);
        return this;
    }

    /**
     * Specify which constructor is to be called when {@link #newInstance(Object...)} is invoked. If the constructor
     * cannot be found the configured {@link ErrorHandler} will be invoked.
     *
     * @param parameterTypes array of parameterTypes which the constructor uses.
     * @return {@code this}
     * @see Class#getConstructor(Class...)
     */
    public ProClass<T> useConstructorFor(Class<?>... parameterTypes) {
        try {
            m_constructor = m_proClass.getConstructor(parameterTypes);
        } catch (NoSuchMethodException e) {
            m_errorHandler.handle("Unable to find constructor for  " + m_proClass.getName() + " with parameterTypes: "
                    + Arrays.toString(parameterTypes), e);
        }
        m_loadConstructor = false;
        return this;
    }

    /**
     * Construct a new instance of the pro class using {@code parameters}. If the constructor fails the configured
     * {@link ErrorHandler} will be invoked. If a constructor was not selected by {@link #useConstructorFor(Class...)}
     * then the constructor will be selected using the {@link Class} of each {@code parameter}. If there isn't an exact
     * type match the first constructor found where all parameter types can be assigned to the constructors parameter
     * types will be used. This makes passing {@code null} in as a parameter dangerous since that can be passed in for
     * any object
     *
     * @param parameters array of parameters to pass to the constructor
     * @return new instance of {@code T} or {@code null} if the class or constructor could not be found
     * @see Constructor#newInstance(Object...)
     */
    public T newInstance(Object... parameters) {
        Constructor<T> constructor = m_constructor;
        if (m_loadConstructor) {
            Class<?>[] parameterTypes = new Class[parameters.length];
            for (int i = 0; i < parameters.length; ++i) {
                parameterTypes[i] = parameters[i] == null ? null : parameters[i].getClass();
            }
            constructor = findConstructor(parameterTypes);
        }
        if (constructor != null) {
            try {
                return constructor.newInstance(parameters);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException e) {
                m_errorHandler.handle(
                        "Unable to construct " + m_proClass.getName() + " with parameters: "
                                + Arrays.toString(parameters),
                        e instanceof InvocationTargetException ? e.getCause() : e);
            }
        }
        return null;
    }

    /**
     * Reset the constructor which was set by {@link #useConstructorFor(Class...)}
     *
     * @return {@code this}
     */
    public ProClass<T> resetConstructor() {
        m_constructor = null;
        m_loadConstructor = true;
        return this;
    }

    /**
     * @return {@code true} if the pro class was loaded
     */
    public boolean hasProClass() {
        return m_proClass != null;
    }

    @SuppressWarnings("unchecked")
    private Constructor<T> findConstructor(Class<?>... parameterTypes) {
        try {
            return m_proClass.getConstructor(parameterTypes);
        } catch (NoSuchMethodException e) {
        }

        Constructor<?> result = null;
        for (Constructor<?> constructor : m_proClass.getConstructors()) {
            int compatibility = checkCompatibility(parameterTypes, constructor);
            if (compatibility == 0) {
                result = constructor;
                break;
            } else if (result == null && compatibility == 1) {
                result = constructor;
            }
        }

        if (result == null) {
           m_errorHandler.handle("Unable to find constructor for  " + m_proClass.getName() + " with parameterTypes: "
                   + Arrays.toString(parameterTypes), null);
        }
        return (Constructor<T>) result;
    }

    /**
     * Check if {@code parameterTypes} are compatible with the {@code executable}
     *
     * @param parameterTypes which are going to be passed into the executable
     * @param executable     which is being tested for compatibility
     * @return {@code -1} if the types are not compatible, {@code 0} if they are an exact match ignoring primitive
     *         wrapping or {@code 1} if they are compatible
     */
    private int checkCompatibility(Class<?>[] parameterTypes, Executable executable) {
        if (executable.getParameterCount() != parameterTypes.length) {
            return -1;
        }
        Class<?>[] executableParameterTypes = executable.getParameterTypes();
        int result = 0;
        for (int i = 0; i < parameterTypes.length; ++i) {
            if (parameterTypes[i] == null) {
                if (executableParameterTypes[i].isPrimitive()) {
                    return -1;
                }
                result = 1;
            } else {
                Class<?> wrapped = Primitives.wrap(executableParameterTypes[i]);
                if (wrapped != parameterTypes[i]) {
                    if (!wrapped.isAssignableFrom(parameterTypes[i])) {
                        return -1;
                    }
                    result = 1;
                }
            }
        }
        return result;
    }

    /**
     * Interface for handling errors which occur while loading or constructing pro classes
     */
    public interface ErrorHandler {
        /**
         * @param message   detailed error message describing when the error occurred
         * @param throwable which was thrown
         */
        void handle(String message, Throwable throwable);
    };
}
