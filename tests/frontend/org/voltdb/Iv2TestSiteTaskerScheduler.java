/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import org.junit.Test;

import org.voltdb.jni.ExecutionEngine;
import org.voltdb.SiteTasker;
import org.voltdb.SiteTaskerScheduler;

import junit.framework.TestCase;

public class Iv2TestSiteTaskerScheduler extends TestCase
{

    static class Task extends SiteTasker
    {
        boolean run = false;
        final int priority;

        Task(int priority)
        {
            this.priority = priority;
        }

        @Override
        public void run(ExecutionEngine ee)
        {
            run = true;
        }

        @Override
        public int priority()
        {
            return priority;
        }
    }

    @Test
    public void testSimpleRoundTrip()
    {
        SiteTaskerScheduler sts = new SiteTaskerScheduler();
        Task t1 = new Task(0);

        SiteTasker r = sts.poll();
        assertTrue("Empty queue returns null", r == null);
        sts.offer(t1);
        r = sts.poll();
        assertTrue("Round trip one task", r == t1);
    }

    @Test
    public void testComparator()
    {
        SiteTaskerScheduler.TaskComparator cmp =
            new SiteTaskerScheduler.TaskComparator();

        Task p1 = new Task(1);
        p1.setSeq(0);

        Task p2 = new Task(2);
        p2.setSeq(1);

        Task p3 = new Task(1);
        p3.setSeq(2);

        assertTrue("1 = 1", cmp.compare(p1, p1) == 0);
        assertTrue("1 < 2", cmp.compare(p1, p2) < 0);
        assertTrue("2 > 1", cmp.compare(p2, p1) > 0);
        assertTrue("Seq order", cmp.compare(p1, p3) < 0);
        assertTrue("Seq order-1", cmp.compare(p3, p1) > 0);
    }


    @Test
    public void testBaseCase()
    {
        SiteTaskerScheduler sts = new SiteTaskerScheduler();
        Task p1 = new Task(1);
        Task p2 = new Task(2);

        sts.offer(p1);
        sts.offer(p2);
        assertSame("Basecase-1", sts.poll(), p1);
        assertSame("Basecase-2", sts.poll(), p2);

        sts.offer(p2);
        sts.offer(p1);
        assertSame("Basecase-3", sts.poll(), p1);
        assertSame("Basecase-4", sts.poll(), p2);
    }

    @Test
    public void testPrioritization()
    {
        SiteTaskerScheduler sts = new SiteTaskerScheduler();
        Task p1 = new Task(1);
        Task p2 = new Task(2);
        Task p3 = new Task(3);
        Task p2_2 = new Task(2);

        sts.offer(p2);
        sts.offer(p1);
        sts.offer(p3);
        sts.offer(p2_2);

        assertSame("Prioritization-1", sts.poll(), p1);
        assertSame("Prioritization-2", sts.poll(), p2);
        assertSame("Prioritization-2,2", sts.poll(), p2_2);
        assertSame("Prioritization-3", sts.poll(), p3);
    }

    @Test
    public void testEqualPriorities()
    {
        SiteTaskerScheduler sts = new SiteTaskerScheduler();
        Task t1 = new Task(0);
        Task t2 = new Task(0);
        Task t3 = new Task(0);

        sts.offer(t1);
        sts.offer(t2);
        sts.offer(t3);

        assertTrue("Insertion order maintained", sts.poll() == t1);
        assertTrue("Insertion order maintained", sts.poll() == t2);
        assertTrue("Insertion order maintained", sts.poll() == t3);
    }

}
