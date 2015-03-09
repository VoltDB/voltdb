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
#include "JNILogProxy.h"
#include <cassert>
#include <stdlib.h>
#include <iostream>

namespace voltdb {

/**
 * Private constructor that only stores value. getJNILogProxy does all the heavy JNI lifting
 */
JNILogProxy::JNILogProxy(JNIEnv *env, JavaVM *vm, jclass eeLoggersClass, jmethodID logMethodID) :
    m_vm(vm), m_env(env), m_eeLoggersClass(eeLoggersClass), m_logMethodID(logMethodID) {
}

/**
 * Factory method that constructs a JNI log proxy using the supplied JNIEnv and JavaVM.
 * Does all the heavy JNI lifting retrieving class and method ids.
 */
JNILogProxy* JNILogProxy::getJNILogProxy(JNIEnv *env, JavaVM *vm) {
#ifdef DEBUG
    union {
        JNIEnv* checkEnv;
        void* env_p;
    };
    checkEnv = NULL;
    jint result = vm->GetEnv( &env_p, JNI_VERSION_1_2);
    assert(result == JNI_OK);
    assert(env == checkEnv);
#endif
    jclass eeLoggersClassLocal = env->FindClass("org/voltdb/jni/EELoggers");
    jclass eeLoggersClass;
    //std::cout << "Looking for class: " << "org/voltdb/jni/EELoggers" << std::endl;
    if (eeLoggersClassLocal == NULL) {
        std::cout << std::endl << "**********************exception found****************" << std::endl;
        env->ExceptionDescribe();
        exit(-1);
    } else {
        eeLoggersClass = static_cast<jclass>(env->NewGlobalRef(eeLoggersClassLocal));
        env->DeleteLocalRef(eeLoggersClassLocal);
        //std::cout << "Found class" << std::endl;
    }

    jmethodID logMethodID = env->GetStaticMethodID( eeLoggersClass, "log", "(IILjava/lang/String;)V");
    if (logMethodID == NULL) {
        std::cout << std::endl << "**********************exception found****************" << std::endl;
        env->ExceptionDescribe();
        exit(-1);
    } else {
        //std::cout << "Found logMethodID method" << std::endl;
    }
    return new JNILogProxy(env, vm, eeLoggersClass, logMethodID);
}

/**
 * Log a statement on behalf of the specified logger at the specified log level
 * @param LoggerId ID of the logger that received this statement
 * @param level Log level of the statement
 * @param statement null terminated UTF-8 string containing the statement to log
 */
void JNILogProxy::log(LoggerId loggerId, LogLevel level, const char *statement) const {
#ifdef DEBUG
    union {
        JNIEnv* checkEnv;
        void* env_p;
    };
    checkEnv = NULL;
    jint result = const_cast<JavaVM*>(m_vm)->GetEnv( &env_p, JNI_VERSION_1_2);
    assert(result == JNI_OK);
    assert(m_env == checkEnv);
#endif
    jstring jStatement = m_env->NewStringUTF(statement);
    if (jStatement == NULL) {
        m_env->ExceptionDescribe();
        exit(-1);
    }
    m_env->CallStaticVoidMethod(m_eeLoggersClass, m_logMethodID, static_cast<jint>(loggerId), static_cast<jint>(level), jStatement);
    if (m_env->ExceptionCheck()) {
        m_env->ExceptionDescribe();
        exit(-1);
    }
    m_env->DeleteLocalRef(jStatement);
}

/**
 * Destructor that frees the GlobalReference m_eeLoggersClass.
 */
JNILogProxy::~JNILogProxy() {
    union {
        JNIEnv* env;
        void* env_p;
    };
    env = NULL;
    jint result = const_cast<JavaVM*>(m_vm)->GetEnv( &env_p, JNI_VERSION_1_2);
    if (result == JNI_OK) {
        env->DeleteGlobalRef(m_eeLoggersClass);
    }
}
}

