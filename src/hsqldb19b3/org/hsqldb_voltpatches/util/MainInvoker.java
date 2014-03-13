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


package org.hsqldb_voltpatches.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

/**
 * Invokes the static main(String[]) method from each class specified.
 *
 * This class <b>will System.exit()</b> if any invocation fails.
 *
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 * @since    HSQLDB 1.8.0
 * @version  $Revision: 766 $, $Date: 2009-01-11 21:12:17 -0500 (Sun, 11 Jan 2009) $
 */
public class MainInvoker {

    /*
     * This class currently consists of just a static utility.
     * It may or may not make sense to make this into a class with real
     * instances that can keep track of status of stuff invoked by it.
     */
    private static String[] emptyStringArray = new String[0];

    private static void syntaxFailure() {
        System.err.println(SYNTAX_MSG);
        System.exit(2);
    }

    /**
     * Invokes the static main(String[]) method from each specified class.
     * This method <b>will System.exit()</b> if any invocation fails.
     *
     * Note that multiple class invocations are delimited by empty-string
     * parameters.  How the user supplies these empty strings is determined
     * entirely by the caller's environment.  From Windows this can
     * generally be accomplished with double-quotes like "".  From all
     * popular UNIX shells, this can be accomplished with single or
     * double-quotes:  '' or "".
     *
     * @param sa Run java org.hsqldb_voltpatches.util.MainInvoker --help for syntax help
     */
    public static void main(String[] sa) {

        if (sa.length > 0 && sa[0].equals("--help")) {
            System.err.println(SYNTAX_MSG);
            System.exit(0);
        }

        ArrayList outList  = new ArrayList();
        int       curInArg = -1;

        try {
            while (++curInArg < sa.length) {
                if (sa[curInArg].length() < 1) {
                    if (outList.size() < 1) {
                        syntaxFailure();
                    }

                    invoke((String) outList.remove(0),
                           (String[]) outList.toArray(emptyStringArray));
                    outList.clear();
                } else {
                    outList.add(sa[curInArg]);
                }
            }

            if (outList.size() < 1) {
                syntaxFailure();
            }

            invoke((String) outList.remove(0),
                   (String[]) outList.toArray(emptyStringArray));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static String LS = System.getProperty("line.separator");
    private static String SYNTAX_MSG =
        "    java org.hsqldb_voltpatches.util.MainInvoker "
        + "[package1.Class1 [arg1a arg1b...] \"\"]... \\\n"
        + "    packageX.ClassX [argXa argXb...]\n" + "OR\n"
        + "    java org.hsqldb_voltpatches.util.MainInvoker --help\n\n"
        + "Note that you can only invoke classes in 'named' (non-default) "
        + "packages.  Delimit multiple classes with empty strings.";
    static {
        if (!LS.equals("\n")) {
            SYNTAX_MSG = SYNTAX_MSG.replaceAll("\n", LS);
        }
    }

    /**
     * Invokes the static main(String[]) method from each specified class.
     */
    public static void invoke(String className,
                              String[] args)
                              throws ClassNotFoundException,
                                     NoSuchMethodException,
                                     IllegalAccessException,
                                     InvocationTargetException {

        Class    c;
        Method   method;
        Class[]  stringArrayCA = { emptyStringArray.getClass() };
        Object[] objectArray   = { (args == null) ? emptyStringArray
                                                  : args };

        c      = Class.forName(className);
        method = c.getMethod("main", stringArrayCA);

        method.invoke(null, objectArray);

        //System.err.println(c.getName() + ".main() invoked");
    }
}
