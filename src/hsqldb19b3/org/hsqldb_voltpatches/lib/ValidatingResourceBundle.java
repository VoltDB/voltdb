/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.lib;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/* $Id: ValidatingResourceBundle.java 5221 2013-03-30 10:57:58Z fredt $ */

/**
 * Purpose of this class is to wrap a RefCapablePropertyResourceBundle to
 *  reliably detect any possible use of a missing property key as soon as
 *  this class is clinitted.
 * The reason for this is to allow us developers to detect all such errors
 *  before end-users ever use this class.
 *
 * See SqltoolRB for an example implementation of this abstract class.
 */
public class ValidatingResourceBundle {
    protected boolean validated = false;
    protected Class<? extends Enum<?>> enumType;

    public static final int THROW_BEHAVIOR =
            RefCapablePropertyResourceBundle.THROW_BEHAVIOR;
    public static final int EMPTYSTRING_BEHAVIOR =
            RefCapablePropertyResourceBundle.EMPTYSTRING_BEHAVIOR;
    public static final int NOOP_BEHAVIOR =
            RefCapablePropertyResourceBundle.NOOP_BEHAVIOR;
    /* Three constants above are only so caller doesn't need to know
     * details of RefCapablePropertyResourceBundle (and they won't need
     * to code that God-awfully-long class name). */

    protected RefCapablePropertyResourceBundle wrappedRCPRB;

    public static String resourceKeyFor(Enum<?> enumKey) {
        return enumKey.name().replace('_', '.');
    }

    public ValidatingResourceBundle(
            String baseName, Class<? extends Enum<?>> enumType) {
        this.enumType = enumType;
        try {
            wrappedRCPRB = RefCapablePropertyResourceBundle.getBundle(baseName,
                    enumType.getClassLoader());
            validate();
        } catch (RuntimeException re) {
            System.err.println("Failed to initialize resource bundle: " + re);
            // Make extra sure that the source of this fatal startup condition
            // is not hidden.
            throw re;
        }
    }

    // The following methods are a passthru wrappers for the wrapped RCPRB.

    /** @see RefCapablePropertyResourceBundle#getString(String) */
    public String getString(Enum<?> key) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return wrappedRCPRB.getString(key.toString());
    }

    /** @see RefCapablePropertyResourceBundle#getString(String, String[], int) */
    public String getString(Enum<?> key, String... strings) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return wrappedRCPRB.getString(
                key.toString(), strings, missingPosValueBehavior);
    }

    /** @see RefCapablePropertyResourceBundle#getExpandedString(String, int) */
    public String getExpandedString(Enum<?> key) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return wrappedRCPRB.getExpandedString(key.toString(), missingPropertyBehavior);
    }

    /** @see RefCapablePropertyResourceBundle#getExpandedString(String, String[], int, int) */
    public String getExpandedString(Enum<?> key, String... strings) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return wrappedRCPRB.getExpandedString(key.toString(), strings,
                missingPropertyBehavior, missingPosValueBehavior);
    }

    private int missingPropertyBehavior = THROW_BEHAVIOR;
    private int missingPosValueBehavior = THROW_BEHAVIOR;

    /**
     * Set behavior for get*String*() method when a referred-to
     * System Property is not set.  Set to one of
     * <UL>
     *  <LI>RefCapablePropertyResourceBundle.THROW_BEHAVIOR
     *  <LI>RefCapablePropertyResourceBundle.EMPTYSTRING_BEHAVIOR
     *  <LI>RefCapablePropertyResourceBundle.NOOP_BEHAVIOR
     * </UL>
     * The first value is the default.
     */
    public void setMissingPropertyBehavior(int missingPropertyBehavior) {
        this.missingPropertyBehavior = missingPropertyBehavior;
    }
    /**
     * Set behavior for get*String(String, String[]) method when a
     * positional index (like %{4}) is used but no subs value was given for
     * that index.  Set to one of
     * <UL>
     *  <LI>RefCapablePropertyResourceBundle.THROW_BEHAVIOR
     *  <LI>RefCapablePropertyResourceBundle.EMPTYSTRING_BEHAVIOR
     *  <LI>RefCapablePropertyResourceBundle.NOOP_BEHAVIOR
     * </UL>
     * The first value is the default.
     */
    public void setMissingPosValueBehavior(int missingPosValueBehavior) {
        this.missingPosValueBehavior = missingPosValueBehavior;
    }

    public int getMissingPropertyBehavior() {
        return missingPropertyBehavior;
    }
    public int getMissingPosValueBehavior() {
        return missingPosValueBehavior;
    }

    public void validate() {
        String val;
        if (validated) return;
        validated = true;
        Set<String> resKeysFromEls = new HashSet<String>();
        for (Enum<?> e : enumType.getEnumConstants())
            resKeysFromEls.add(e.toString());
        Enumeration<String> allKeys = wrappedRCPRB.getKeys();
        while (allKeys.hasMoreElements()) {
            // We can't test positional parameters, but we can verify that
            // referenced files exist by reading the values.
            // Pretty inefficient, but this can be optimized when I have time.
            val = allKeys.nextElement();
            wrappedRCPRB.getString(val);  // because it throws if missing?
            // Keep no reference to the returned String
            resKeysFromEls.remove(val);
        }
        if (resKeysFromEls.size() > 0)
            throw new RuntimeException(
                    "Resource Bundle pre-validation failed.  "
                    + "Missing property with key:  " + resKeysFromEls);
    }

    /* Convenience wrappers follow for getString(int, String[]) for up to
     * 3 int and/or String positionals or any number of just String positions
     */
    public String getString(Enum<?> key, int i1) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {Integer.toString(i1)});
    }
    public String getString(Enum<?> key, int i1, int i2) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), Integer.toString(i2)
        });
    }
    public String getString(Enum<?> key, int i1, int i2, int i3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), Integer.toString(i2), Integer.toString(i3)
        });
    }
    public String getString(Enum<?> key, int i1, String s2) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), s2
        });
    }
    public String getString(Enum<?> key, String s1, int i2) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            s1, Integer.toString(i2)
        });
    }

    public String getString(Enum<?> key, int i1, int i2, String s3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), Integer.toString(i2), s3
        });
    }
    public String getString(Enum<?> key, int i1, String s2, int i3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), s2, Integer.toString(i3)
        });
    }
    public String getString(Enum<?> key, String s1, int i2, int i3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            s1, Integer.toString(i2), Integer.toString(i3)
        });
    }
    public String getString(Enum<?> key, int i1, String s2, String s3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), s2, s3
        });
    }
    public String getString(Enum<?> key, String s1, String s2, int i3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            s1, s2, Integer.toString(i3)
        });
    }
    public String getString(Enum<?> key, String s1, int i2, String s3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            s1, Integer.toString(i2), s3
        });
    }
}
