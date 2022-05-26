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

#ifndef JNILOGPROXY_H_
#define JNILOGPROXY_H_
#include "LogDefs.h"
#include "LogProxy.h"
#include <jni.h>
#include <common/debuglog.h>

namespace voltdb {

/**
 * An implementation of a LogProxy that forwards the log statements to the java org.voltdb.jni.EELoggers class
 * using JNI.
 */
class JNILogProxy : public LogProxy {
public:

    /**
     * Log a statement on behalf of the specified logger at the specified log level
     * @param LoggerId ID of the logger that received this statement
     * @param level Log level of the statement
     * @param statement null terminated UTF-8 string containing the statement to log
     */
    void log(LoggerId loggerId, LogLevel level, const char *statement) const;

    /**
     * Update the reference to the JNIEnv object that is used to invoke java logging methods.
     * The env pointer must be updated EVERY time there is a transition from Java to native
     * code where this log proxy might be used. The env pointer is unique to each thread
     * and can change every time there is a transition from Java to C.
     */
    inline void setJNIEnv(JNIEnv *env) {
        m_env = env;
    }

    /**
     * Factory method that constructs a JNI log proxy using the supplied JNIEnv and JavaVM.
     * Does all the heavy JNI lifting retrieving class and method ids.
     */
    static JNILogProxy* getJNILogProxy(JNIEnv *env, JavaVM *vm);

    /**
     * Destructor that frees the GlobalReference m_eeLoggersClass.
     */
    virtual ~JNILogProxy();
private:
    /**
     * Private constructor that only stores value. getJNILogProxy does all the heavy JNI lifting
     */
    JNILogProxy(JNIEnv *env, JavaVM *vm, jclass m_eeLoggersClass, jmethodID logMethodID);

    /**
     * Pointer to the JavaVM object for this process. Used for debug purposes to assert that the currently
     * stored JNIEnv pointer is the correct.
     */
    const JavaVM *m_vm;

    /**
     * Reference to the current JNIEnv object for this thread. Must be kept up to date.
     */
    JNIEnv *m_env;

    /**
     * GlobalReference to org.voltdb.jni.EELoggers that is used to call the static log method.
     * GlobalReference is deleted in the destructor.
     */
    const jclass m_eeLoggersClass;

    /**
     * ID of the static log method in org.voltdb.jni.EELoggers.
     * MethodIDs do not need to be deleted/freed unlike Global and Local JNI references.
     */
    const jmethodID m_logMethodID;
};
}

#endif /* JNILOGPROXY_H_ */
