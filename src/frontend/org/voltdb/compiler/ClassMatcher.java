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

package org.voltdb.compiler;

import java.io.File;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Functionality to build a list of every class that's in
 * the classpath as a class file and not a jar. Also the
 * ability to match pattens against the big list to get a
 * set of matching classes out.
 *
 */
public class ClassMatcher {
    public enum ClassNameMatchStatus {
        MATCH_FOUND,
        NO_WILDCARD_MATCH,
        NO_EXACT_MATCH,
    }

    /** List of .class files found in the classpath */
    public String m_classList = null;
    /** List of matches found after applying patterns */
    SortedSet<String> m_classNameMatches = new TreeSet<String>();

    /**
     * Add a pattern that matches classes from the classpath
     * and add any matching classnames to m_classNameMatches.
     *
     * The pattern is of the form "org.voltdb.Foo" but can
     * contain single and double asterisks in the style
     * of ant wildcards, such as "org.voltdb.**" or
     * "org.voltdb.*bar" or "org.volt**.Foo"
     */
    public ClassNameMatchStatus addPattern(String classNamePattern) {
        boolean matchFound = false;
        if (m_classList == null) {
            m_classList = getClasspathClassFileNames();
        }

        String preppedName = classNamePattern.trim();

        // include only full classes
        // for nested classes, include the parent pattern
        int indexOfDollarSign = classNamePattern.indexOf('$');
        if (indexOfDollarSign >= 0) {
            classNamePattern = classNamePattern.substring(0, indexOfDollarSign);
        }

        // Substitution order is critical.
        // Keep track of whether or not this is a wildcard expression.
        // '.' is specifically not a wildcard.
        String regExPreppedName = preppedName.replace(".", "[.]");
        boolean isWildcard = regExPreppedName.contains("*");
        if (isWildcard) {
            regExPreppedName = regExPreppedName.replace("**", "[\\w.\\$]+");
            regExPreppedName = regExPreppedName.replace("*",  "[\\w\\$]*");
        }

        String regex = "^" + // (line start)
                       regExPreppedName +
                       "$";  // (line end)

        Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

        Matcher matcher = pattern.matcher(m_classList);
        while (matcher.find()) {
            String match = matcher.group();
            // skip nested classes; the base class will include them
            if (match.contains("$")) {
                continue;
            }
            matchFound = true;
            m_classNameMatches.add(match);
        }
        if (matchFound) {
            return ClassNameMatchStatus.MATCH_FOUND;
        }
        else {
            if (isWildcard) {
                return ClassNameMatchStatus.NO_WILDCARD_MATCH;
            }
            else {
                return ClassNameMatchStatus.NO_EXACT_MATCH;
            }
        }

    }

    /**
     * Return the set of matched classnames in lexographical order.
     */
    public SortedSet<String> getMatchedClassList() {
        return m_classNameMatches;
    }

    /**
     * Empty the data structures of this class to save memory.
     */
    public void clear() {
        m_classList = null;
        m_classNameMatches.clear();
    }

    /**
     * Helper class to process a directory of classfiles.
     */
    private static class Package {
        final File file;
        final Package parent;

        Package(Package parent, File file) {
            assert(file != null);

            this.file = file;
            this.parent = parent;
        }

        /** Return the "dot" name for the class */
        String getJavaName() {
            String fullName = file.getName();
            for (Package p = parent; p != null; p = p.parent) {
                String parentName = p.file.getName();
                fullName = parentName + "." + fullName;
            }
            return fullName;
        }

        /**
         * Process all of the elements inside a directory
         * into sub-packages and classfiles, adding classfiles
         * to the given parameter, "classes".
         */
        void process(Set<String> classes) {
            File[] files = file.listFiles();
            for (File f : files) {
                String name = f.getName();

                if (name.endsWith(".class")) {
                    String className = name.substring(0, name.length() - ".class".length());
                    String javaName = getJavaName() + "." + className;
                    classes.add(javaName);
                }

                else if (f.isDirectory()) {
                    Package p = new Package(this, f);
                    p.process(classes);
                }
            }
        }
    }

    /**
     * For a given classpath root, scan it for packages and classes,
     * adding all found classnames to the given "classes" param.
     */
    private static void processPathPart(String path, Set<String> classes) {
        File rootFile = new File(path);
        if (rootFile.isDirectory() == false) {
            return;
        }

        File[] files = rootFile.listFiles();
        for (File f : files) {
            // classes in the anonymous package
            if (f.getName().endsWith(".class")) {
                String className = f.getName();
                // trim the trailing .class from the end
                className = className.substring(0, className.length() - ".class".length());
                classes.add(className);
            }
            if (f.isDirectory()) {
                Package p = new Package(null, f);
                p.process(classes);
            }
        }
    }

    /**
     * Get a single string that contains all of the non-jar
     * classfiles in the current classpath, separated by
     * newlines. Classfiles are represented by their Java
     * "dot" names, not filenames.
     */
    static String getClasspathClassFileNames() {
        String classpath = System.getProperty("java.class.path");
        String[] pathParts = classpath.split(File.pathSeparator);

        Set<String> classes = new TreeSet<String>();

        for (String part : pathParts) {
            processPathPart(part, classes);
        }

        StringBuilder sb = new StringBuilder();
        for (String className : classes) {
            sb.append(className).append('\n');
        }

        return sb.toString();
    }
}
