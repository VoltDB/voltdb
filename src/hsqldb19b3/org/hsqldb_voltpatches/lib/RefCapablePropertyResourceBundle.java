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


package org.hsqldb_voltpatches.lib;

import java.util.PropertyResourceBundle;
import java.util.Map;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.MissingResourceException;
import java.util.Enumeration;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.InputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;


/* $Id: RefCapablePropertyResourceBundle.java 826 2009-01-17 05:04:52Z unsaved $ */

/**
 * Just like PropertyResourceBundle, except keys mapped to nothing in the
 * properties file will load the final String value from a text file.
 *
 * The use case is where one wants to use a ResourceBundle for Strings,
 * but some of the Strings are long-- too long to maintain in a Java
 * .properties file.
 * By using this class, you can put each such long String in its own
 * separate file, yet all keys mapped to (non-empty) values in the
 * .properties file will behave just like regular PropertyResourceBundle
 * properties.
 * In this documentation, I call these values read in atomically from
 * other files <i>referenced</i> values, because the values are not directly
 * in the .properties file, but are "referenced" in the .properties file
 * by virtue of the empty value for the key.
 *
 * You use this class in the same way as you would traditionally use
 * ResourceBundle:
 * <PRE>
 *  import org.hsqldb_voltpatches.util..RefCapablePropertyResourceBundle;
 *  ...
 *      RefCapablePropertyResourceBundle bundle =
 *              RefCapablePropertyResourceBundle.getBundle("subdir.xyz");
 *      System.out.println("Value for '1' = (" + bundle.getString("1") + ')');
 * </PRE>
 *
 * Just like PropertyResourceBundle, the .properties file and the
 * <i>referenced</i> files are read in from the classpath by a class loader,
 * according to the normal ResourceBundle rules.
 * To eliminate the need to prohibit the use of any strings in the .properties
 * values, and to enforce consistency, you <b>must</b> use the following rules
 * to when putting your referenced files into place.
 * <P/>
 * REFERENCED FILE DIRECTORY is a directory named with the base name of the
 * properties file, and in the same parent directory.  So, the referenced
 * file directory <CODE>/a/b/c/greentea</CODE> is used to hold all reference
 * files for properties files <CODE>/a/b/c/greentea_en_us.properties</CODE>,
 * <CODE>/a/b/c/greentea_de.properties</CODE>,
 * <CODE>/a/b/c/greentea.properties</CODE>, etc.
 * (BTW, according to ResourceBundle rules, this resource should be looked
 * up with name "a.b.c.greentea", not "/a/b/c..." or "a/b/c").
 * REFERENCED FILES themselves all have the base name of the property key,
 * with locale appendages exactly as the <i>referring</i> properties files
 * has, plus the suffix <CODE>.text</CODE>.
 * <P/>
 * So, if we have the following line in
 * <CODE>/a/b/c/greentea_de.properties</CODE>:
 * <PRE>
 *     1: eins
 * </PRE>
 * then you <b>must</b> have a reference text file
 * <CODE>/a/b/c/greentea/1_de.properties</CODE>:
 * <P/>
 * In reference text files,
 * sequences of "\r", "\n" and "\r\n" are all translated to the line
 * delimiter for your platform (System property <CODE>line.separator</CODE>).
 * If one of those sequences exists at the very end of the file, it will be
 * eliminated (so, if you really want getString() to end with a line delimiter,
 * end your file with two of them).
 * (The file itself is never modified-- I'm talking about the value returned
 * by <CODE>getString(String)</CODE>).
 * <P/>
 * To prevent throwing at runtime due to unset variables, use a wrapper class
 * like SqltoolRB (use SqltoolRB.java as a template).
 * To prevent throwing at runtime due to unset System Properties, or
 * insufficient parameters passed to getString(String, String[]), set the
 * behavior values appropriately.
 * <P/>
 * Just like all Properties files, referenced files must use ISO-8859-1
 * encoding, with unicode escapes for characters outside of ISO-8859-1
 * character set.  But, unlike Properties files, \ does not need to be
 * escaped for normal usage.
 * <P/>
 * The getString() methods with more than one parameter substitute for
 * "positional" parameters of the form "%{1}".
 * The getExpandedString() methods substitute for System Property names
 * of the form "${1}".
 * In both cases, you can interpose :+ and a string between the variable
 * name and the closing }.  This works just like the Bourne shell
 * ${x:+y} feature.  If "x" is set, then "y" is returned, and "y" may
 * contain references to the original variable without the curly braces.
 * In this file, I refer to the y text as the "conditional string".
 * One example of each type:
 * <PRE>
 *     Out val = (${condlSysProp:+Prop condlSysProp is set to $condlSysProp.})
 *     Out val = (%{2:+Pos Var #2 is set to %2.})
 * OUTPUT if neither are set:
 *     Out val = ()
 *     Out val = ()
 * OUTPUT if condlSysProp=alpha and condlPLvar=beta:
 *     Out val = (Prop condlSysProp is set to alpha.)
 *     Out val = (Pos Var #2 is set to beta.)
 * </PRE>
 * This feature has the following limitations.
 * <UL>
 *   <LI>The conditional string may only contain the primary variable.
 *   <LI>Inner instances of the primary variable may not use curly braces,
 *       and therefore the variable name must end at a word boundary.
 * </UL>
 * The conditional string may span newlines, and it is often very useful
 * to do so.
 *
 * @see java.util.PropertyResourceBundle
 * @see java.util.ResourceBundle
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 */
public class RefCapablePropertyResourceBundle {
    private PropertyResourceBundle wrappedBundle;
    private String baseName;
    private String language, country, variant;
    static private Map allBundles = new HashMap();
    public static String LS = System.getProperty("line.separator");
    private Pattern sysPropVarPattern = Pattern.compile(
            "(?s)\\Q${\\E([^}]+?)(?:\\Q:+\\E([^}]+))?\\Q}");
    private Pattern posPattern = Pattern.compile(
            "(?s)\\Q%{\\E(\\d)(?:\\Q:+\\E([^}]+))?\\Q}");
    private ClassLoader loader;  // Needed to load referenced files

    public static final int THROW_BEHAVIOR = 0;
    public static final int EMPTYSTRING_BEHAVIOR = 1;
    public static final int NOOP_BEHAVIOR = 2;

    public Enumeration getKeys() {
        return wrappedBundle.getKeys();
    }

    private RefCapablePropertyResourceBundle(String baseName,
            PropertyResourceBundle wrappedBundle, ClassLoader loader) {
        this.baseName = baseName;
        this.wrappedBundle = wrappedBundle;
        Locale locale = wrappedBundle.getLocale();
        this.loader = loader;
        language = locale.getLanguage();
        country = locale.getCountry();
        variant = locale.getVariant();
        if (language.length() < 1) language = null;
        if (country.length() < 1) country = null;
        if (variant.length() < 1) variant = null;
    }

    /**
     * Same as getString(), but expands System Variables specified in
     * property values like ${sysvarname}.
     */
    public String getExpandedString(String key, int behavior) {
        String s = getString(key);
        Matcher matcher = sysPropVarPattern.matcher(s);
        int previousEnd = 0;
        StringBuffer sb = new StringBuffer();
        String varName, varValue;
        String condlVal;  // Conditional : value
        while (matcher.find()) {
            varName = matcher.group(1);
            condlVal = ((matcher.groupCount() > 1) ? matcher.group(2) : null);
            varValue = System.getProperty(varName);
            if (condlVal != null) {
                // Replace varValue (the value to be substituted), with
                // the post-:+ portion of the expression.
                varValue = ((varValue == null)
                        ? ""
                        : condlVal.replaceAll("\\Q$" + varName + "\\E\\b",
                                RefCapablePropertyResourceBundle.literalize(
                                        varValue)));
            }
            if (varValue == null) switch (behavior) {
                case THROW_BEHAVIOR:
                    throw new RuntimeException(
                            "No Sys Property set for variable '"
                            + varName + "' in property value ("
                            + s + ").");
                case EMPTYSTRING_BEHAVIOR:
                    varValue = "";
                case NOOP_BEHAVIOR:
                    break;
                default:
                    throw new RuntimeException(
                            "Undefined value for behavior: " + behavior);
            }
            sb.append(s.substring(previousEnd, matcher.start())
                        + ((varValue == null) ? matcher.group() : varValue));
            previousEnd = matcher.end();
        }
        return (previousEnd < 1) ? s
                                 : (sb.toString() + s.substring(previousEnd));
    }

    /**
     * Replaces positional substitution patterns of the form %{\d} with
     * corresponding element of the given subs array.
     * Note that %{\d} numbers are 1-based, so we lok for subs[x-1].
     */
    public String posSubst(String s, String[] subs, int behavior) {
        Matcher matcher = posPattern.matcher(s);
        int previousEnd = 0;
        StringBuffer sb = new StringBuffer();
        String varValue;
        int varIndex;
        String condlVal;  // Conditional : value
        while (matcher.find()) {
            varIndex = Integer.parseInt(matcher.group(1)) - 1;
            condlVal = ((matcher.groupCount() > 1) ? matcher.group(2) : null);
            varValue = ((varIndex < subs.length) ? subs[varIndex] : null);
            if (condlVal != null) {
                // Replace varValue (the value to be substituted), with
                // the post-:+ portion of the expression.
                varValue = ((varValue == null)
                        ? ""
                        : condlVal.replaceAll("\\Q%" + (varIndex+1) + "\\E\\b",
                                RefCapablePropertyResourceBundle.literalize(
                                        varValue)));
            }
            // System.err.println("Behavior: " + behavior);
            if (varValue == null) switch (behavior) {
                case THROW_BEHAVIOR:
                    throw new RuntimeException(
                            Integer.toString(subs.length)
                            + " positional values given, but property string "
                            + "contains (" + matcher.group() + ").");
                case EMPTYSTRING_BEHAVIOR:
                    varValue = "";
                case NOOP_BEHAVIOR:
                    break;
                default:
                    throw new RuntimeException(
                            "Undefined value for behavior: " + behavior);
            }
            sb.append(s.substring(previousEnd, matcher.start())
                        + ((varValue == null) ? matcher.group() : varValue));
            previousEnd = matcher.end();
        }
        return (previousEnd < 1) ? s
                                 : (sb.toString() + s.substring(previousEnd));
    }

    public String getExpandedString(String key, String[] subs,
            int missingPropertyBehavior, int missingPosValueBehavior) {
        return posSubst(getExpandedString(key, missingPropertyBehavior), subs,
                missingPosValueBehavior);
    }
    public String getString(String key, String[] subs, int behavior) {
        return posSubst(getString(key), subs, behavior);
    }

    /**
     * Just identifies this RefCapablePropertyResourceBundle instance.
     */
    public String toString() {
        return baseName + " for " + language + " / " + country + " / "
            + variant;
    }

    /**
     * Returns value defined in this RefCapablePropertyResourceBundle's
     * .properties file, unless that value is empty.
     * If the value in the .properties file is empty, then this returns
     * the entire contents of the referenced text file.
     *
     * @see ResourceBundle#getString(String)
     */
    public String getString(String key) {
        String value = wrappedBundle.getString(key);
        if (value.length() < 1) {
            value = getStringFromFile(key);
            // For conciseness and sanity, get rid of all \r's so that \n
            // will definitively be our line breaks.
            if (value.indexOf('\r') > -1)
                value = value.replaceAll("\\Q\r\n", "\n")
                        .replaceAll("\\Q\r", "\n");
            if (value.length() > 0 && value.charAt(value.length() - 1) == '\n')
                value = value.substring(0, value.length() - 1);
        }
        return RefCapablePropertyResourceBundle.toNativeLs(value);
    }

    /**
     * @param inString  Input string with \n definitively indicating desired
     *                  position for line separators.
     * @return  If platform's line-separator is \n, then just returns inString.
     *           Otherwise returns a copy of inString, with all \n's
     *           transformed to the platform's line separators.
     */
    static public String toNativeLs(String inString) {
        return LS.equals("\n") ? inString : inString.replaceAll("\\Q\n", LS);
    }

    /**
     * Use like java.util.ResourceBundle.getBundle(String).
     *
     * ClassLoader is required for our getBundles()s, since it is impossible
     * to get the "caller's" ClassLoader without using JNI (i.e., with pure
     * Java).
     *
     * @see ResourceBundle#getBundle(String)
     */
    public static RefCapablePropertyResourceBundle getBundle(String baseName,
            ClassLoader loader) {
        return getRef(baseName, ResourceBundle.getBundle(baseName,
                Locale.getDefault(), loader), loader);
    }
    /**
     * Use exactly like java.util.ResourceBundle.get(String, Locale, ClassLoader).
     *
     * @see ResourceBundle#getBundle(String, Locale, ClassLoader)
     */
    public static RefCapablePropertyResourceBundle
            getBundle(String baseName, Locale locale, ClassLoader loader) {
        return getRef(baseName,
                ResourceBundle.getBundle(baseName, locale, loader), loader);
    }

    /**
     * Return a ref to a new or existing RefCapablePropertyResourceBundle,
     * or throw a MissingResourceException.
     */
    static private RefCapablePropertyResourceBundle getRef(String baseName,
            ResourceBundle rb, ClassLoader loader) {
        if (!(rb instanceof PropertyResourceBundle))
            throw new MissingResourceException(
                    "Found a Resource Bundle, but it is a "
                            + rb.getClass().getName(),
                    PropertyResourceBundle.class.getName(), null);
        if (allBundles.containsKey(rb))
            return (RefCapablePropertyResourceBundle) allBundles.get(rb);
        RefCapablePropertyResourceBundle newPRAFP =
                new RefCapablePropertyResourceBundle(baseName,
                        (PropertyResourceBundle) rb, loader);
        allBundles.put(rb, newPRAFP);
        return newPRAFP;
    }

    /**
     * Recursive
     */
    private InputStream getMostSpecificStream(
            String key, String l, String c, String v) {
        String filePath = baseName.replace('.', '/') + '/' + key
                + ((l == null) ? "" : ("_" + l))
                + ((c == null) ? "" : ("_" + c))
                + ((v == null) ? "" : ("_" + v))
                + ".text";
        // System.err.println("Seeking " + filePath);
        InputStream is = loader.getResourceAsStream(filePath);
        // N.b.  If were using Class.getRes... instead of ClassLoader.getRes...
        // we would need to prefix the path with "/".
        return (is == null && l != null)
            ? getMostSpecificStream(key, ((c == null) ? null : l),
                    ((v == null) ? null : c), null)
            : is;
    }

    private String getStringFromFile(String key) {
        byte[] ba = null;
        int bytesread = 0;
        int retval;
        InputStream  inputStream =
                getMostSpecificStream(key, language, country, variant);
        if (inputStream == null)
            throw new MissingResourceException(
                    "Key '" + key
                    + "' is present in .properties file with no value, yet "
                    + "text file resource is missing",
                    RefCapablePropertyResourceBundle.class.getName(), key);
        try {
            try {
                ba = new byte[inputStream.available()];
            } catch (RuntimeException re) {
                throw new MissingResourceException(
                    "Resource is too big to read in '" + key + "' value in one "
                    + "gulp.\nPlease run the program with more RAM "
                    + "(try Java -Xm* switches).: " + re,
                    RefCapablePropertyResourceBundle.class.getName(), key);
            } catch (IOException ioe) {
                throw new MissingResourceException(
                    "Failed to read in value for key '" + key + "': " + ioe,
                    RefCapablePropertyResourceBundle.class.getName(), key);
            }
            try {
                while (bytesread < ba.length &&
                        (retval = inputStream.read(
                                ba, bytesread, ba.length - bytesread)) > 0) {
                    bytesread += retval;
                }
            } catch (IOException ioe) {
                throw new MissingResourceException(
                    "Failed to read in value for '" + key + "': " + ioe,
                    RefCapablePropertyResourceBundle.class.getName(), key);
            }
        } finally {
            try {
                inputStream.close();
            } catch (IOException ioe) {
                System.err.println("Failed to close input stream: " + ioe);
            }
        }
        if (bytesread != ba.length) {
            throw new MissingResourceException(
                    "Didn't read all bytes.  Read in "
                      + bytesread + " bytes out of " + ba.length
                      + " bytes for key '" + key + "'",
                    RefCapablePropertyResourceBundle.class.getName(), key);
        }
        try {
            return new String(ba, "ISO-8859-1");
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException(uee);
        } catch (RuntimeException re) {
            throw new MissingResourceException(
                "Value for key '" + key + "' too big to convert to String.  "
                + "Please run the program with more RAM "
                + "(try Java -Xm* switches).: " + re,
                RefCapablePropertyResourceBundle.class.getName(), key);
        }
    }

    /**
     * Escape \ and $ characters in replacement strings so that nothing
     * funny happens.
     *
     * Once we can use Java 1.5, wipe out this method and use
     * java.util.regex.matcher.QuoteReplacement() instead.
     */
    public static String literalize(String s) {
        if ((s.indexOf('\\') == -1) && (s.indexOf('$') == -1)) {
            return s;
        }
        StringBuffer sb = new StringBuffer();
        for (int i=0; i<s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    sb.append('\\'); sb.append('\\');
                    break;
                case '$':
                    sb.append('\\'); sb.append('$');
                    break;
                default:
                    sb.append(c);
                    break;
            }
        }
        return sb.toString();
    }
}
