/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.utils;

import java.util.Iterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.voltdb.utils.AdaptivePriorityQueue.OrderingPolicy;

import junit.framework.TestCase;

public class TestAdaptivePriorityQueue extends TestCase {

    public static class PrioritizedInteger implements Prioritized{
        final int priority;
        final int value;

        public PrioritizedInteger(int prio, int value){
            this.priority = prio;
            this.value = value;
        }

        @Override
        public int getPriority() {
            return priority;
        }

        @Override
        public void setPriority(int priority) {
            throw new UnsupportedOperationException();
        }
        public int getValue() {
            return value;
        }
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testNoPriority() {

        int max_priority = AdaptivePriorityQueue.MAX_PRIORITY_LEVEL;
        int max_count = 5 * (max_priority+1);
        AdaptivePriorityQueue<PrioritizedInteger> aQueue = new AdaptivePriorityQueue<>(OrderingPolicy.NO_PRIORITY_POLICY);
        int count = 0;
        while(count < max_count){
            for(int ii = 0; ii<max_priority+1; ii++) {
                aQueue.offer(new PrioritizedInteger(ii,count++));
            }
        }

        assertEquals(max_count,aQueue.size());

        Iterator<PrioritizedInteger> iter = aQueue.iterator();
        count = 0;
        while( iter.hasNext() ){
            PrioritizedInteger cur = iter.next();
            int expectedPriority = count % (max_priority+1);
            assertEquals(expectedPriority,cur.getPriority() );
            assertEquals(count,cur.getValue());
            count++;
        }

        iter = aQueue.insertionOrderIterator();
        count = 0;
        while( iter.hasNext() ){
            PrioritizedInteger cur = iter.next();
            int expectedPriority = count % (max_priority+1);
            assertEquals(expectedPriority, cur.getPriority());
            assertEquals(count,cur.getValue());
            count++;
        }

        count = 0;
        while( !aQueue.isEmpty() ){
            PrioritizedInteger cur = aQueue.poll();
            int expectedPriority = count % (max_priority+1);
            assertEquals(expectedPriority,cur.getPriority());
            assertEquals(count,cur.getValue());
            count++;
        }

    }

    @Test
    public void testUserDefinedPriority() {
        int cPr = 5;
        int max_priority = AdaptivePriorityQueue.MAX_PRIORITY_LEVEL;
        int max_count = cPr * (max_priority+1);
        AdaptivePriorityQueue<PrioritizedInteger> aQueue = new AdaptivePriorityQueue<>(OrderingPolicy.USER_DEFINED_POLICY);
        int count = 0;
        while(count < max_count){
            for(int ii = 0; ii<max_priority+1; ii++) {
                aQueue.offer(new PrioritizedInteger(ii,count++));
            }
        }

        assertEquals(aQueue.size(),max_count);

        Iterator<PrioritizedInteger> iter = aQueue.iterator();
        count = 0;
        while( iter.hasNext() ){
            PrioritizedInteger cur = iter.next();
            int expectedPriority = count / cPr ;
            int level = count % cPr;
            int expectedSecond = (max_priority+1) * level + expectedPriority;
            assertEquals(expectedPriority,cur.getPriority());
            assertEquals(expectedSecond,cur.getValue());
            count++;
        }
        assertEquals(max_count,count);

        count = 0;
        while( !aQueue.isEmpty() ){
            PrioritizedInteger cur = aQueue.poll();
            int expectedPriority = count/cPr;
            int level = count % cPr;
            int expectedSecond = (max_priority+1) * level + expectedPriority;
            assertEquals(expectedPriority,cur.getPriority());
            assertEquals(expectedSecond,cur.getValue());
            count++;
        }
        assertEquals(max_count,count);

        //somehow this does not work
        //thrown.expect(UnsupportedOperationException.class);
        //aQueue.insertionOrderIterator();
        //use below try/catch instead
        boolean failed = false;
        try {
            aQueue.insertionOrderIterator();
        }
        catch( UnsupportedOperationException ex) {
            failed = true;
        }
        assertTrue(failed);

        assertEquals(aQueue.size(),0);
    }

    @Test
    public void testUserDefinedPriority10() {
        int cPr = 5;
        int max_priority = 9;
        int max_count = cPr * (max_priority+1);
        AdaptivePriorityQueue<PrioritizedInteger> aQueue = new AdaptivePriorityQueue<>(OrderingPolicy.USER_DEFINED_POLICY);
        int count = 0;
        while(count < max_count){
            for(int ii = 0; ii<max_priority+1; ii++) {
                aQueue.offer(new PrioritizedInteger(ii,count++));
            }
        }

        assertEquals(aQueue.size(),max_count);

        Iterator<PrioritizedInteger> iter = aQueue.iterator();
        count = 0;
        while( iter.hasNext() ){
            PrioritizedInteger cur = iter.next();
            int expectedPriority = count / cPr;
            int level = count % cPr;
            int expectedSecond = (max_priority+1) * level + expectedPriority;
            assertEquals(expectedPriority,cur.getPriority());
            assertEquals(expectedSecond,cur.getValue());
            count++;
        }
        assertEquals(max_count,count);

        // empty out 6 highest priorities
        int n_req = 6;
        count = 0;
        for( ; count < n_req*cPr; count++ ){
            PrioritizedInteger cur = aQueue.poll();
            int expectedPriority = count / cPr;
            int level = count % cPr;
            int expectedSecond = (max_priority+1) * level + expectedPriority;
            assertEquals(expectedPriority,cur.getPriority());
            assertEquals(expectedSecond,cur.getValue());
        }

        // add additional values at the highest level
        int NN = 7;
        for(int ii=0; ii < NN; ii++){
            aQueue.offer(new PrioritizedInteger((ii+1)%2,ii));
        }
        // Now make sure that this highest priority are next
        // we should have 3 elements w Priority 1
        for( int ii = 0; ii < 3; ii++){
            PrioritizedInteger cur = aQueue.poll();
            assertEquals(0,cur.getPriority());
            assertEquals(ii*2 + 1,cur.getValue());
        }
        for( int ii = 0; ii < 4; ii++){
            PrioritizedInteger cur = aQueue.poll();
            assertEquals(1,cur.getPriority());
            assertEquals(ii*2,cur.getValue());
        }

        // Now get remaining elements
        count = n_req * cPr;
        while( aQueue.peek() != null ){
            PrioritizedInteger cur = aQueue.poll();
            int expectedPriority = count / cPr;
            int level = count % cPr;
            int expectedSecond = (max_priority+1) * level + expectedPriority;
            assertEquals(expectedPriority,cur.getPriority());
            assertEquals(expectedSecond,cur.getValue());
            count++;
        }

        assertEquals(aQueue.size(),0);
    }

    //TODO Add reverse insertion test. start inserting from lowest priority toward the top.
    @Test
    public void testUserDefinedPriorityReverse() {
        int cPr = 5;
        int max_priority = 9;
        int max_count = cPr * (max_priority+1);
        AdaptivePriorityQueue<PrioritizedInteger> aQueue = new AdaptivePriorityQueue<>(OrderingPolicy.USER_DEFINED_POLICY);
        int count = 0;
        while(count < max_count){
            for(int ii = max_priority+1; ii>0; ii--) {
                aQueue.offer(new PrioritizedInteger(ii-1,ii*1000+count));
                count++;
             }
        }
        assertEquals(max_count,aQueue.size());

        int[] expectedOrder = new int[] {  1009, 1019, 1029, 1039, 1049,
                                           2008, 2018, 2028, 2038, 2048,
                                           3007, 3017, 3027, 3037, 3047,
                                           4006, 4016, 4026, 4036, 4046,
                                           5005, 5015, 5025, 5035, 5045,
                                           6004, 6014, 6024, 6034, 6044,
                                           7003, 7013, 7023, 7033, 7043,
                                           8002, 8012, 8022, 8032, 8042,
                                           9001, 9011, 9021, 9031, 9041,
                                          10000,10010,10020,10030,10040 };

        Iterator<PrioritizedInteger> iter = aQueue.iterator();
        count = 0;
        while( iter.hasNext() ){
            PrioritizedInteger cur = iter.next();
            assertEquals(expectedOrder[count++],cur.getValue());
        }
        assertEquals(max_count,count);

        // empty out 6 highest priorities
        int n_req = 6;
        count = 0;
        for( ; count < n_req*cPr; count++ ){
            PrioritizedInteger cur = aQueue.poll();
            assertEquals(expectedOrder[count],cur.getValue());
        }

        // add additional values at the highest level
        int NN = 7;
        for(int ii=0; ii < NN; ii++){
            aQueue.offer( new PrioritizedInteger((ii+1)%2 , ((ii+1)%2+1) * 100 + ii) );
        }
        // Now make sure that this highest priority are next
        // we should have 3 elements w Priority 1
        for( int ii = 0; ii < 3; ii++){
            PrioritizedInteger cur = aQueue.poll();
            assertEquals(100+ii*2+1,cur.getValue());
        }
        for( int ii = 0; ii < 4; ii++){
            PrioritizedInteger cur = aQueue.poll();
            assertEquals(200+ii*2,cur.getValue());
        }

        // Now get remaining elements
        count = n_req * cPr;
        while( aQueue.peek() != null ){
            PrioritizedInteger cur = aQueue.poll();
            assertEquals(expectedOrder[count++],cur.getValue());
        }
        assertEquals(aQueue.size(),0);
    }


    @Test
    public void testUserDefinedPriorityReinsert() {
        AdaptivePriorityQueue<PrioritizedInteger> aQueue = new AdaptivePriorityQueue<>(OrderingPolicy.USER_DEFINED_POLICY);

        // add additional values at the highest level
        int NN = 7;
        for(int ii=0; ii < NN; ii++){
            aQueue.offer(new PrioritizedInteger((ii+1)%2,ii));
        }
        // Now make sure that this highest priority are next
        // we should have 3 elements w Priority 1
        PrioritizedInteger cur;
        for( int ii = 0; ii < 3; ii++){
            cur = aQueue.poll();
            assertEquals(0,cur.getPriority());
            assertEquals(ii*2 + 1,cur.getValue());
        }

        // This should empty out all priority 0.
        // Add 1 element w Priority 0 back
        aQueue.offer(new PrioritizedInteger(0,1001));
        //get it back
        cur = aQueue.poll();
        assertEquals(0,cur.getPriority());
        assertEquals(1001,cur.getValue());
        // Insert 2 more elements w priority 0 & 1;
        aQueue.offer(new PrioritizedInteger(1,1002));
        aQueue.offer(new PrioritizedInteger(0,1003));
        // Check remaining elements
        cur = aQueue.poll();
        assertEquals(0,cur.getPriority());
        assertEquals(1003,cur.getValue());

        for( int ii = 0; ii < 4; ii++){
            cur = aQueue.poll();
            assertEquals(1,cur.getPriority());
            assertEquals(ii*2,cur.getValue());
        }

        cur = aQueue.poll();
        assertEquals(1,cur.getPriority());
        assertEquals(1002,cur.getValue());

        assertEquals(aQueue.size(),0);
    }

    @Test
    public void testQueueElementFactory() {
        AdaptivePriorityQueue.poolIncrement = 10;
        AdaptivePriorityQueue.poolInitialCapacity = 20;

        int cPr = 5;
        int max_priority = 31;
        int max_count = cPr * (max_priority+1);
        AdaptivePriorityQueue<PrioritizedInteger> aQueue = new AdaptivePriorityQueue<>(OrderingPolicy.USER_DEFINED_POLICY);

        //initially the pool should be "full"
        assertEquals(aQueue.size(),0);
        //pool should be empty by this point
        assertEquals(AdaptivePriorityQueue.poolIncrement,aQueue.getPoolSize());

        int count = 0;
        while(count < max_count){
            for(int ii = 0; ii<max_priority+1; ii++) {
                aQueue.offer(new PrioritizedInteger(ii,count++));
            }
        }

        assertEquals(aQueue.size(),max_count);
        //pool should be empty by this point
        assertEquals(0,aQueue.getPoolSize());

        count = 0;
        while( !aQueue.isEmpty() ){
            PrioritizedInteger cur = aQueue.poll();
            int expectedPriority = count / cPr;
            int level = count % cPr;
            int expectedSecond = (max_priority+1) * level + expectedPriority;
            assertEquals(expectedPriority,cur.getPriority());
            assertEquals(expectedSecond,cur.getValue());
            count++;
        }

        // so the queue should be empty now.
        // On the other hand, the pool should get all elements back and its size should be equeal max_count
        assertEquals(0,aQueue.size());
        assertEquals(max_count,aQueue.getPoolSize());

        //let insert max_count/2 elements back into the queue. Those should be taken from the pool now.
        count=0;
        for(int jj=0; jj<3; jj++){
            for(int ii = 0; ii<max_priority+1; ii++) {
                aQueue.offer(new PrioritizedInteger(ii,count++));
            }
        }
        assertEquals(count,aQueue.size());
        assertEquals(max_count-count,aQueue.getPoolSize());
        assertEquals(160, aQueue.getPoolCapacity());
    }

    @Test
    public void testMaxWaitPriority() throws InterruptedException {
        int cPr = 5;
        int max_priority = 9;
        int max_count = cPr * (max_priority+1) + 20;
        AdaptivePriorityQueue<PrioritizedInteger> aQueue = new AdaptivePriorityQueue<>(OrderingPolicy.MAX_WAIT_POLICY);
        int count = 0;
        // add 10 elements at priority 1;
        for(;count < 10; count ++){
            aQueue.offer(new PrioritizedInteger(1,count));
        }
        //add 10 elements with priority 8 & 9 first, 5 each
        for(;count < 20; count ++){
            aQueue.offer(new PrioritizedInteger((count+1)%2+8,count));
        }
        //Sleep 700 msec before inserting remaining
        //At one point in the test these elements will time-out
        Thread.sleep(700);

        // add remaining elements
        while(count < max_count){
            for(int ii = 0; ii<max_priority+1; ii++) {
                aQueue.offer(new PrioritizedInteger(ii,count++));
            }
        }
        assertEquals(max_count,aQueue.size());

        //Validate insertion order iterator
        Iterator<PrioritizedInteger> iter = aQueue.insertionOrderIterator();
        int jj = 0;
        while( iter.hasNext() ){
            PrioritizedInteger cur = iter.next();
            assertEquals(jj++,cur.getValue());
         }

        //Validate priority iterators
        int[] expectedOrder = new int[] {20,30,40,50,60,      //priority 1
                                          0, 1, 2, 3, 4, 5, 6, 7, 8,9,21,31,41,51,61,  //priority 2
                                         22,32,42,52,62,      //priority 3
                                         23,33,43,53,63,      //priority 4
                                         24,34,44,54,64,      //priority 5
                                         25,35,45,55,65,      //priority 6
                                         26,36,46,56,66,      //priority 7
                                         27,37,47,57,67,      //priority 8
                                         11,13,15,17,19,28,38,48,58,68,      //priority 9
                                         10,12,14,16,18,29,39,49,59,69};      //priority 10
        jj = 0;
        iter = aQueue.iterator();
        while( iter.hasNext() ){
            PrioritizedInteger cur = iter.next();
            assertEquals(expectedOrder[jj++],cur.getValue());
        }

        // empty out 6 highest priorities
        int n_req = 6;
        for( int ii = 0; ii < n_req*cPr+10; ii++ ){
            assertFalse(aQueue.hasTimeOutElement());
            PrioritizedInteger cur = aQueue.poll();
            assertEquals(expectedOrder[ii],cur.getValue());
        }

        // wait 0.7 second so that some elements times out
        Thread.sleep(700);
        assertTrue(aQueue.hasTimeOutElement());

        // add additional values at the highest priority
        int NN = 7;
        for(int ii=0; ii < NN; ii++){
            aQueue.offer(new PrioritizedInteger((ii+1)%2,count++));
        }

        // Now inspite that there are plenty of "high" priority elements,
        // next we should get time-out elements at level 9 & 10, priority 10 will arrive first
        // we expect to see elements <10,10>, <9,11>, <10, 12>, etc
        for( int ii = 10; ii < 20; ii++ ){
            PrioritizedInteger cur = aQueue.poll();
            //System.out.println(cur);
            assertEquals(ii,cur.getValue());
            assertEquals((ii+1)%2+8,cur.getPriority());
        }
        // should not have any more time out...
        assertFalse(aQueue.hasTimeOutElement());

        //Get remaining elements with newly inserted elements should appear first.
        expectedOrder = new int[] { 71,73,75,          //priority 1
                                    70,72,74,76,       // priority 2
                                    26,36,46,56,66,    // priority 7
                                    27,37,47,57,67,    // priority 8
                                    28,38,48,58,68,    // priority 9
                                    29,39,49,59,69};   // priority 10
        count = 0;
        while( aQueue.peek() != null ){
            PrioritizedInteger cur = aQueue.poll();
            //System.out.println(cur);
            assertEquals(expectedOrder[count++],cur.getValue());
        }

        assertEquals(aQueue.size(),0);
    }

    @Test
    public void testFreqPriority2() {

        int cPr = 20;
        int max_priority = 1;
        int max_count = cPr * (max_priority+1);
        AdaptivePriorityQueue<PrioritizedInteger> aQueue = new AdaptivePriorityQueue<>(OrderingPolicy.FREQUENCY_DEFINED_POLICY,120000000);

        // Insert records sequentially. One record w priority 0 and another w priority 1
        for (int ii = 0; ii < cPr; ii++) {
            for( int jj = 0; jj < 2; jj++) {
                int id = (jj+1) * 1000 + ii;
                aQueue.offer(new PrioritizedInteger(jj,id));
            }
        }
        assertEquals(max_count,aQueue.size());

        //Validate priority iterators
        int[] expectedOrder = new int[] { 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009,
                                          1010, 2000, 2001, 1011, 2002, 2003, 1012, 2004, 2005, 1013,
                                          2006, 2007, 1014, 2008, 2009, 1015, 2010, 2011, 1016, 2012,
                                          2013, 1017, 2014, 2015, 1018, 2016, 2017, 1019, 2018, 2019};

        for( int count = 0; count < max_count; count++ ){
            PrioritizedInteger cur = aQueue.poll();
            assertEquals(expectedOrder[count],cur.getValue());
        }
        assertEquals(0,aQueue.size());

        // Check what expected order if Priority 1 is inserted before Priority 0:
        // A smaller number of Priority 0 elements appear before priority 1 start being processed.
        for( int jj = 1; jj >= 0; jj--) {
            for (int ii = 0; ii < cPr; ii++) {
                int id = (jj+1) * 1000 + ii;
                aQueue.offer(new PrioritizedInteger(jj,id));
            }
        }

        assertEquals(max_count,aQueue.size());

        //Validate priority iterators
        expectedOrder = new int[] { 2000, 1000, 1001, 2001, 2002, 2003, 2004, 1002, 1003, 2005,
                                    2006, 2007, 2008, 1004, 1005, 2009, 2010, 2011, 2012, 1006,
                                    1007, 2013, 2014, 2015, 2016, 1008, 1009, 2017, 2018, 2019,
                                    1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019 };

        for( int count = 0; count < max_count; count++ ){
            PrioritizedInteger cur = aQueue.poll();
            assertEquals( cur.getValue(),expectedOrder[count]);
        }
        assertEquals(0,aQueue.size());
    }

    @Test
    public void testFreqPriority3() throws InterruptedException {

        int cPr = 20;
        int max_priority = 2;
        int max_count = cPr * (max_priority+1);
        AdaptivePriorityQueue<PrioritizedInteger> aQueue = new AdaptivePriorityQueue<>(OrderingPolicy.FREQUENCY_DEFINED_POLICY,120000000);

        for (int ii = 0; ii < cPr; ii++) {
            for( int jj = 0; jj < 3; jj++) {
                int id = (jj+1) * 1000 + ii;
                aQueue.offer(new PrioritizedInteger(jj,id));
            }
        }
        assertEquals(max_count,aQueue.size());

        //Validate priority iterators
        int[] expectedOrder = new int[] { 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009,
                                          1010, 2000, 2001, 1011, 2002, 2003, 1012, 2004, 2005, 1013,
                                          2006, 2007, 3000, 3001, 1014, 2008, 3002, 3003, 2009, 1015,
                                          3004, 3005, 2010, 2011, 3006, 3007, 1016, 2012, 3008, 3009,
                                          2013, 1017, 3010, 3011, 2014, 2015, 3012, 3013, 1018, 2016,
                                          3014, 3015, 2017, 1019, 3016, 3017, 2018, 2019, 3018, 3019  };

        for(int count = 0; count < max_count; count++ ){
            PrioritizedInteger cur = aQueue.poll();
            assertEquals( expectedOrder[count],cur.getValue());
        }
        assertEquals(0,aQueue.size());

        // Check what expected order if Priority 2 is inserted before Priority 1  before Priority 0:
        // A smaller number of Priority 0 elements appear before priority 1 start being processed.
        for( int jj = 2; jj >= 0; jj--) {
            for (int ii = 0; ii < cPr; ii++) {
                int id = (jj+1) * 1000 + ii;
                aQueue.offer(new PrioritizedInteger(jj,id));
            }
        }
        assertEquals(max_count,aQueue.size());

        //Validate priority iterators
        expectedOrder = new int[] { 3000, 2000, 1000, 1001, 3001, 3002, 3003, 2001, 2002, 2003,
                                    2004, 1002, 3004, 3005, 3006, 3007, 3008, 3009, 2005, 2006,
                                    1003, 1004, 3010, 3011, 3012, 2007, 2008, 2009, 2010, 1005,
                                    3013, 3014, 3015, 3016, 3017, 3018, 2011, 2012, 1006, 1007,
                                    3019, 2013, 2014, 2015, 2016, 1008, 1009, 2017, 2018, 2019,
                                    1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019 };

        for( int count = 0; count < max_count; count++ ){
            PrioritizedInteger cur = aQueue.poll();
            assertEquals( cur.getValue(),expectedOrder[count]);
        }
        assertEquals(0,aQueue.size());
    }

}
