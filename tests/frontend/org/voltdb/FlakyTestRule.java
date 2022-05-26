/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import static org.junit.Assume.assumeTrue;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class FlakyTestRule implements TestRule {
    // Print debug statements only if -Drun.flaky.tests.debug=TRUE
    private final boolean DEBUG = FlakyTestStandardRunner.debug();

    /**
     * The @Flaky annotation is intended to be used for JUnit tests that do not
     * pass reliably, failing either intermittently or consistently; such tests
     * may or may not be skipped, depending on several factors:<br>
     *     o The value of the system property run.flaky.tests (set on the command
     *       line via '-Drun.flaky.tests=...'), when running the tests.<br>
     *     o The value of the <i>isFlaky</i> parameter, e.g. @Flaky(isFlaky=true), which
     *       is the default value.<br>
     *     o If you choose, rather than using <i>FlakyTestStandardRunner</i>,
     *       which is the the default runCondition parameter, to write your own
     *       implementation of FlakyTestRunner, then it could depend on whatever
     *       other factors you choose.<br>
     * Once a test passes reliably again, the @Flaky annotation may be removed,
     * or changed to use @Flaky(isFlaky=false). There is also an optional
     * <i>description</i> parameter, which may be used to describe the test or
     * the ways and circumstances in which it is flaky.
     */
    @Documented
    @Target(ElementType.METHOD)
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Flaky {
        boolean isFlaky() default true;
        String description() default "";
        Class<? extends FlakyTestRunner> runCondition() default FlakyTestStandardRunner.class;
    }

    /**
     * Interface to determine, via either of its runFlakyTest methods, whether or not a
     * particular @Flaky test (that is, a JUnit test with an @Flaky annotation)
     * should be run.
     * @param testIsFlaky boolean: the value of a test's @Flaky annotation's
     * 'isFlaky' parameter
     * @param description String: the value of a test's @Flaky annotation's
     * 'description' parameter
     */
    public interface FlakyTestRunner {
        /**
         * Used to determine whether or not a particular @Flaky test (that is,
         * a JUnit test with an @Flaky annotation) should be run.
         * @param testIsFlaky boolean: the value of a test's @Flaky annotation's
         * 'isFlaky' parameter
         */
        boolean runFlakyTest(boolean testIsFlaky);
        /**
         * Used to determine whether or not a particular @Flaky test (that is,
         * a JUnit test with an @Flaky annotation) should be run.
         * @param testIsFlaky boolean: the value of a test's @Flaky annotation's
         * 'isFlaky' parameter
         * @param description String: the value of a test's @Flaky annotation's
         * 'description' parameter
         */
        boolean runFlakyTest(boolean testIsFlaky, String description);
    }

    /**
     * Implements TestRule.apply: modifies the method-running Statement per this
     * test-running rule, which determines whether or not this particular JUnit
     * test with an @Flaky annotation should be Ignored (i.e., not run).
     * @param base The Statement to (perhaps) be modified
     * @param desc A Description of the test implemented in <i>base</i>
     */
    @Override
    public Statement apply(Statement base, Description desc) {
        if (DEBUG) {
            System.out.println("DEBUG: In FlakyTestRule.apply:");
            System.out.println("DEBUG:   base: "+base);
            System.out.println("DEBUG:   desc: "+desc);
        }

        Statement result = base;
        Flaky flakyAnnotation = desc.getAnnotation(Flaky.class);
        if (flakyAnnotation != null) {
            FlakyTestRunner runCondition = (new FlakyTestRunnerCreator(desc, flakyAnnotation)).create();
            if (!runCondition.runFlakyTest(flakyAnnotation.isFlaky(), flakyAnnotation.description())) {
                result = new IgnoreThisTest();
            }
        }
        return result;
    }

    private static class FlakyTestRunnerCreator {
        private final Description description;
        private final Class<? extends FlakyTestRunner> runCondition;
        // Print debug statements only if -Drun.flaky.tests.debug=TRUE
        private final boolean DEBUG = FlakyTestStandardRunner.debug();

        FlakyTestRunnerCreator(Description description, Flaky flakyAnnotation) {
            if (DEBUG) {
                System.out.println("DEBUG: In FlakyTestRule.FTRCreator constructor");
            }
            this.description = description;
            this.runCondition = flakyAnnotation.runCondition();
        }

        FlakyTestRunner create() {
            if (DEBUG) {
                System.out.println("DEBUG: In FlakyTestRule.FTRCreator.create");
            }
            checkConditionType();
            try {
                return createRunCondition();
            } catch (RuntimeException re) {
                throw re;
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        private FlakyTestRunner createRunCondition() throws Exception {
            if (DEBUG) {
                System.out.println("DEBUG: In FlakyTestRule.FTRCreator.createRunCondition");
            }
            FlakyTestRunner result;
            if (isConditionTypeStandalone()) {
                if (DEBUG) {
                    System.out.println("DEBUG:   isConditionTypeStandalone true");
                }
                result = runCondition.newInstance();
            } else {
                Constructor<? extends FlakyTestRunner> declaredConstructor = runCondition.getDeclaredConstructor(description.getTestClass());
                if (DEBUG) {
                    System.out.println("DEBUG:   isConditionTypeStandalone false");
                    System.out.println("DEBUG:     runCondition : "+runCondition);
                    System.out.println("DEBUG:     getAnnotation: "+runCondition.getAnnotation(Flaky.class));
                    System.out.println("DEBUG:     description  : "+description);
                    System.out.println("DEBUG:     getAnnotation: "+description.getAnnotation(Flaky.class));
                    System.out.println("DEBUG:     getTestClass : "+description.getTestClass());
                    System.out.println("DEBUG:     declaredConstructor: "+declaredConstructor);
                    System.out.println("DEBUG:     runCondition : "+runCondition);
                    System.out.println("DEBUG:     runCondition : "+runCondition);
                }
                result = declaredConstructor.newInstance(description);
            }
            if (DEBUG) {
                System.out.println("DEBUG:   result: "+result);
            }
            return result;
        }

        private void checkConditionType() {
            if (DEBUG) {
                System.out.println("DEBUG:   In FlakyTestRule.FTRCreator.checkConditionType");
            }
            if( !isConditionTypeStandalone() && !isConditionTypeDeclaredInTarget() ) {
                System.out.println("DEBUG:     condidition type not good!");
                String msg
                  = "Conditional class '%s' is a member class "
                  + "but was not declared inside the test case using it.\n"
                  + "Either make this class a static class, "
                  + "standalone class (by declaring it in it's own file) "
                  + "or move it inside the test case using it";
                throw new IllegalArgumentException( String.format ( msg, runCondition.getName() ) );
            }
        }

      private boolean isConditionTypeStandalone() {
          if (DEBUG) {
              System.out.println("DEBUG:   In FlakyTestRule.FTRCreator.isConditionTypeStandalone");
              System.out.println("DEBUG:     runCondition : "+runCondition);
              System.out.println("DEBUG:     isMemberClass: "+runCondition.isMemberClass());
              System.out.println("DEBUG:     getModifiers : "+runCondition.getModifiers());
              System.out.println("DEBUG:     isStatic     : "+Modifier.isStatic(runCondition.getModifiers()));
              System.out.println("DEBUG:     returns      : "+(!runCondition.isMemberClass() || Modifier.isStatic(runCondition.getModifiers())));
          }
          return !runCondition.isMemberClass() || Modifier.isStatic(runCondition.getModifiers());
      }

      private boolean isConditionTypeDeclaredInTarget() {
          if (DEBUG) {
              System.out.println("DEBUG:   In FlakyTestRule.FTRCreator.isConditionTypeDeclaredInTarget");
              System.out.println("DEBUG:     description  : "+description);
              System.out.println("DEBUG:     getTestClass : "+description.getTestClass());
              System.out.println("DEBUG:     runCondition : "+runCondition);
              System.out.println("DEBUG:     getDeclaringClass: "+runCondition.getDeclaringClass());
              System.out.println("DEBUG:     returns      : "+(description.getTestClass().isAssignableFrom(runCondition.getDeclaringClass())));
          }
          return description.getTestClass().isAssignableFrom(runCondition.getDeclaringClass());
      }
    }

    /** A (JUnit) Statement that always indicates that the test should be
        ignored, i.e., not run. */
    private static class IgnoreThisTest extends Statement {
        @Override
        public void evaluate() {
            if (FlakyTestStandardRunner.debug()) {
                System.out.println("DEBUG: In FlakyTestRule.IgnoreThisTest.evaluate");
            }
            assumeTrue(false);
        }
    }

}
