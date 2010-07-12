/* Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb_voltpatches.resources;

import java.lang.reflect.Method;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.hsqldb_voltpatches.lib.HashMap;
import org.hsqldb_voltpatches.lib.HsqlArrayList;

/**
 * A ResourceBundle helper class. <p>
 *
 * Allows clients to get/set locale and get at localized resource bundle
 * content in a resource path independent manner, without having to worry
 * about handling exception states or deal directly with ResourceBundle
 * object instances. Instead, clients recieve numeric handles to the
 * underlying objects.  Rather than causing exception states, missing or
 * inaccessible resources and underlying MissingResource and NullPointer
 * exceptions result in null return values when attempting to retrieve a
 * resource. <p>
 *
 * @author boucherb@users
 * @version 1.7.2
 * @since 1.7.2
 */
public final class BundleHandler {

    /** Used to synchronize access */
    private static final Object mutex = new Object();

    /** The Locale used internally to fetch resource bundles. */
    private static Locale locale = Locale.getDefault();

    /** Map:  Integer object handle => <code>ResourceBundle</code> object. */
    private static HashMap bundleHandleMap = new HashMap();

    /** List whose elements are <code>ResourceBundle</code> objects */
    private static HsqlArrayList bundleList = new HsqlArrayList();

    /**
     * The resource path prefix of the <code>ResourceBundle</code> objects
     * handled by this class.
     */
    private static final String prefix = "org/hsqldb_voltpatches/resources/";

    /** JDK 1.1 compliance */
    private static final Method newGetBundleMethod = getNewGetBundleMethod();

    /** Pure utility class: external construction disabled. */
    private BundleHandler() {}

    /**
     * Getter for property locale. <p>
     *
     * @return Value of property locale.
     */
    public static Locale getLocale() {

        synchronized (mutex) {
            return locale;
        }
    }

    /**
     * Setter for property locale. <p>
     *
     * @param l the new locale
     * @throws IllegalArgumentException when the new locale is null
     */
    public static void setLocale(Locale l) throws IllegalArgumentException {

        synchronized (mutex) {
            if (l == null) {
                throw new IllegalArgumentException("null locale");
            }

            locale = l;
        }
    }

    /**
     * Retrieves an <code>int</code> handle to the <code>ResourceBundle</code>
     * object corresponding to the specified name and current
     * <code>Locale</code>, using the specified <code>ClassLoader</code>. <p>
     *
     * @return <code>int</code> handle to the <code>ResourceBundle</code>
     *        object corresponding to the specified name and
     *        current <code>Locale</code>, or -1 if no such bundle
     *        can be found
     * @param cl The ClassLoader to use in the search
     * @param name of the desired bundle
     */
    public static int getBundleHandle(String name, ClassLoader cl) {

        Integer        bundleHandle;
        ResourceBundle bundle;
        String         bundleName;
        String         bundleKey;

        bundleName = prefix + name;

        synchronized (mutex) {
            bundleKey    = locale.toString() + bundleName;
            bundleHandle = (Integer) bundleHandleMap.get(bundleKey);

            if (bundleHandle == null) {
                try {
                    bundle = getBundle(bundleName, locale, cl);

                    bundleList.add(bundle);

                    bundleHandle = new Integer(bundleList.size() - 1);

                    bundleHandleMap.put(bundleKey, bundleHandle);
                } catch (Exception e) {

                    //e.printStackTrace();
                }
            }
        }

        return bundleHandle == null ? -1
                                    : bundleHandle.intValue();
    }

    /**
     * Retrieves, from the <code>ResourceBundle</code> object corresponding
     * to the specified handle, the <code>String</code> value corresponding
     * to the specified key.  <code>null</code> is retrieved if either there
     *  is no <code>ResourceBundle</code> object for the handle or there is no
     * <code>String</code> value for the specified key. <p>
     *
     * @param handle an <code>int</code> handle to a
     *      <code>ResourceBundle</code> object
     * @param key A <code>String</code> key to a <code>String</code> value
     * @return The String value correspoding to the specified handle and key.
     */
    public static String getString(int handle, String key) {

        ResourceBundle bundle;
        String         s;

        synchronized (mutex) {
            if (handle < 0 || handle >= bundleList.size() || key == null) {
                bundle = null;
            } else {
                bundle = (ResourceBundle) bundleList.get(handle);
            }
        }

        if (bundle == null) {
            s = null;
        } else {
            try {
                s = bundle.getString(key);
            } catch (Exception e) {
                s = null;
            }
        }

        return s;
    }

    /**
     * One-shot initialization of JDK 1.2+ ResourceBundle.getBundle() method
     * having ClassLoader in the signature.
     */
    private static Method getNewGetBundleMethod() {

        Class   clazz;
        Class[] args;

        clazz = ResourceBundle.class;
        args  = new Class[] {
            String.class, Locale.class, ClassLoader.class
        };

        try {
            return clazz.getMethod("getBundle", args);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Retrieves a resource bundle using the specified base name, locale, and
     * class loader. This is a JDK 1.1 compliant substitution for the
     * ResourceBundle method with the same name and signature. If there
     * is a problem using the JDK 1.2 functionality (the class loader is
     * specified non-null and the underlying method is not available or there
     * is a security exception, etc.), then the behaviour reverts to that
     * of JDK 1.1.
     *
     * @param name the base name of the resource bundle, a fully
     *      qualified class name
     * @param locale the locale for which a resource bundle is desired
     * @param cl the class loader from which to load the resource bundle
     */
    public static ResourceBundle getBundle(String name, Locale locale,
                                           ClassLoader cl)
                                           throws NullPointerException,
                                               MissingResourceException {

        if (cl == null) {
            return ResourceBundle.getBundle(name, locale);
        } else if (newGetBundleMethod == null) {
            return ResourceBundle.getBundle(name, locale);
        } else {
            try {
                return (ResourceBundle) newGetBundleMethod.invoke(null,
                        new Object[] {
                    name, locale, cl
                });
            } catch (Exception e) {
                return ResourceBundle.getBundle(name, locale);
            }
        }
    }
}
