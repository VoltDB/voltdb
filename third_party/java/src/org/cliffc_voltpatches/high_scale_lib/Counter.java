/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.cliffc_voltpatches.high_scale_lib;

/**
 * A simple high-performance counter.  Merely renames the extended {@link
 * org.cliffc.high_scale_lib.ConcurrentAutoTable} class to be more obvious.
 * {@link org.cliffc.high_scale_lib.ConcurrentAutoTable} already has a decent
 * counting API.
 *
 * @since 1.5
 * @author Cliff Click
 */

public class Counter extends ConcurrentAutoTable {

  // Add the given value to current counter value.  Concurrent updates will
  // not be lost, but addAndGet or getAndAdd are not implemented because but
  // the total counter value is not atomically updated.
  //public void add( long x );
  //public void decrement();
  //public void increment();

  // Current value of the counter.  Since other threads are updating furiously
  // the value is only approximate, but it includes all counts made by the
  // current thread.  Requires a pass over all the striped counters.
  //public long get();
  //public int  intValue();
  //public long longValue();

  // A cheaper 'get'.  Updated only once/millisecond, but fast as a simple
  // load instruction when not updating.
  //public long estimate_get( );

}

