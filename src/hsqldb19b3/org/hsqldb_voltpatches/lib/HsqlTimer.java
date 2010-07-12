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

import java.util.Date;

/**
 * Facility to schedule tasks for future execution in a background thread.  <p>
 *
 * Tasks may be scheduled for one-time execution or for repeated execution at
 * regular intervals, using either fixed rate or fixed delay policy. <p>
 *
 * This class is a JDK 1.1 compatible implementation required by HSQLDB both
 * because the java.util.Timer class is available only in JDK 1.3+ and because
 * java.util.Timer starves least recently added tasks under high load and
 * fixed rate scheduling, especially when the average actual task duration is
 * greater than the average requested task periodicity. <p>
 *
 * An additional (minor) advantage over java.util.Timer is that this class does
 * not retain a live background thread during periods when the task queue is
 * empty.
 * @author boucherb@users
 * @version 1.8.0.10
 * @since 1.7.2
 */
public final class HsqlTimer implements ObjectComparator, ThreadFactory {

    /** The priority queue for the scheduled tasks. */
    protected final TaskQueue taskQueue = new TaskQueue(16,
        (ObjectComparator) this);

    /** The inner runnable that executes tasks in the background thread. */
    protected final TaskRunner taskRunner = new TaskRunner();

    /** The background thread. */
    protected Thread taskRunnerThread;

    /** The factory that procduces the background threads. */
    protected final ThreadFactory threadFactory;

    /**
     * Whether this timer should disallow all further processing.
     *
     * Once set true, stays true forever.
     */
    protected volatile boolean isShutdown;

    /**
     * Constructs a new HsqlTimer using the default thread factory
     * implementation.
     */
    public HsqlTimer() {
        this(null);
    }

    /**
     * Constructs a new HsqlTimer.
     *
     * Uses the specified thread factory implementation.
     *
     * @param threadFactory the ThreadFactory used to produce this timer's
     *      background threads.  If null, the default implementation supplied
     *      by this class will be used.
     */
    public HsqlTimer(final ThreadFactory threadFactory) {
        this.threadFactory = (threadFactory == null) ? this
                                                     : threadFactory;
    }

    /**
     * Required to back the priority queue for scheduled tasks.
     *
     * @param a the first Task
     * @param b the second Task
     * @return 0 if equal, < 0 if a < b, > 0 if a > b
     */
    public int compare(final Object a, final Object b) {

        final long awhen = ((Task) (a)).getNextScheduled();
        final long bwhen = ((Task) (b)).getNextScheduled();

        return (awhen < bwhen) ? -1
                               : (awhen == bwhen) ? 0
                                                  : 1;
    }

    /**
     * Default ThreadFactory implementation. <p>
     *
     * Contructs a new Thread from the designated runnable, sets its
     * name to "HSQLDB Timer @" + Integer.toHexString(hashCode()),
     * and sets it as a daemon thread. <p>
     *
     * @param runnable used to construct the new Thread.
     * @return a new Thread constructed from the designated runnable.
     */
    public Thread newThread(final Runnable runnable) {

        final Thread thread = new Thread(runnable);

        thread.setName("HSQLDB Timer @" + Integer.toHexString(hashCode()));
        thread.setDaemon(true);

        return thread;
    }

    /**
     * Retrieves the background execution thread. <p>
     *
     * null is returned if there is no such thread. <p>
     *
     * @return the current background thread (may be null)
     */
    public synchronized Thread getThread() {
        return this.taskRunnerThread;
    }

    /**
     * (Re)starts background processing of the task queue.
     *
     * @throws IllegalStateException if this timer is shut down.
     * @see #shutdown()
     * @see #shutdownImmediately()
     */
    public synchronized void restart() throws IllegalStateException {

        if (this.isShutdown) {
            throw new IllegalStateException("isShutdown==true");
        } else if (this.taskRunnerThread == null) {
            this.taskRunnerThread =
                this.threadFactory.newThread(this.taskRunner);

            this.taskRunnerThread.start();
        } else {
            this.taskQueue.unpark();
        }
    }

    /**
     * Causes the specified Runnable to be executed once in the background
     * after the specified delay.
     *
     * @param delay in milliseconds
     * @param runnable the Runnable to execute.
     * @return opaque reference to the internal task
     * @throws IllegalArgumentException if runnable is null
     */
    public Object scheduleAfter(final long delay,
                                final Runnable runnable)
                                throws IllegalArgumentException {

        if (runnable == null) {
            throw new IllegalArgumentException("runnable == null");
        }

        return this.addTask(now() + delay, runnable, 0, false);
    }

    /**
     * Causes the specified Runnable to be executed once in the background
     * at the specified time.
     *
     * @param date time at which to execute the specified Runnable
     * @param runnable the Runnable to execute.
     * @return opaque reference to the internal task
     * @throws IllegalArgumentException if date or runnable is null
     */
    public Object scheduleAt(final Date date,
                             final Runnable runnable)
                             throws IllegalArgumentException {

        if (date == null) {
            throw new IllegalArgumentException("date == null");
        } else if (runnable == null) {
            throw new IllegalArgumentException("runnable == null");
        }

        return this.addTask(date.getTime(), runnable, 0, false);
    }

    /**
     * Causes the specified Runnable to be executed periodically in the
     * background, starting at the specified time.
     *
     * @return opaque reference to the internal task
     * @param period the cycle period
     * @param relative if true, fixed rate sheduling else fixed delay scheduling
     * @param date time at which to execute the specified Runnable
     * @param runnable the Runnable to execute
     * @throws IllegalArgumentException if date or runnable is null, or
     *      period is <= 0
     */
    public Object schedulePeriodicallyAt(final Date date, final long period,
                                         final Runnable runnable,
                                         final boolean relative)
                                         throws IllegalArgumentException {

        if (date == null) {
            throw new IllegalArgumentException("date == null");
        } else if (period <= 0) {
            throw new IllegalArgumentException("period <= 0");
        } else if (runnable == null) {
            throw new IllegalArgumentException("runnable == null");
        }

        return addTask(date.getTime(), runnable, period, relative);
    }

    /**
     * Causes the specified Runnable to be executed periodically in the
     * background, starting after the specified delay.
     *
     * @return opaque reference to the internal task
     * @param period the cycle period
     * @param relative if true, fixed rate sheduling else fixed delay scheduling
     * @param delay in milliseconds
     * @param runnable the Runnable to execute.
     * @throws IllegalArgumentException if runnable is null or period is <= 0
     */
    public Object schedulePeriodicallyAfter(final long delay,
            final long period, final Runnable runnable,
            final boolean relative) throws IllegalArgumentException {

        if (period <= 0) {
            throw new IllegalArgumentException("period <= 0");
        } else if (runnable == null) {
            throw new IllegalArgumentException("runnable == null");
        }

        return addTask(now() + delay, runnable, period, relative);
    }

    /**
     * Shuts down this timer after the current task (if any) completes. <p>
     *
     * After this call, the timer has permanently entered the shutdown state;
     * attempting to schedule any new task or directly restart this timer will
     * result in an  IllegalStateException. <p>
     *
     */
    public synchronized void shutdown() {

        if (!this.isShutdown) {
            this.isShutdown = true;

            this.taskQueue.cancelAllTasks();
        }
    }

    /** for compatiblity with previous version */
    public synchronized void shutDown() {
        shutdown();
    }

    /**
     * Shuts down this timer immediately, interrupting the wait state associated
     * with the current head of the task queue or the wait state internal to
     * the currently executing task, if any such state is currently in effect.
     *
     * After this call, the timer has permanently entered the shutdown state;
     * attempting to schedule any new task or directly restart this timer will
     * result in an IllegalStateException. <p>
     *
     * <b>Note:</b> If the integrity of work performed by a scheduled task
     * may be adversely affected by an unplanned interruption, it is the
     * responsibility of the task's implementation to deal correctly with the
     * possibility that this method is called while such work is in progress,
     * for instance by catching the InterruptedException, completing the work,
     * and then rethrowing the exception.
     */
    public synchronized void shutdownImmediately() {

        if (!this.isShutdown) {
            final Thread runner = this.taskRunnerThread;

            this.isShutdown = true;

            if (runner != null && runner.isAlive()) {
                runner.interrupt();
            }

            this.taskQueue.cancelAllTasks();
        }
    }

    /**
     * Causes the task referenced by the supplied argument to be cancelled.
     * If the referenced task is currently executing, it will continue until
     * finished but will not be rescheduled.
     *
     * @param task a task reference
     */
    public static void cancel(final Object task) {

        if (task instanceof Task) {
            ((Task) task).cancel();
        }
    }

    /**
     * Retrieves whether the specified argument references a cancelled task.
     *
     * @param task a task reference
     * @return true if referenced task is cancelled
     */
    public static boolean isCancelled(final Object task) {
        return (task instanceof Task) ? ((Task) task).isCancelled()
                                      : true;
    }

    /**
     * Retrieves whether the specified argument references a task scheduled
     * periodically using fixed rate scheduling.
     *
     * @param task a task reference
     * @return true if the task is scheduled at a fixed rate
     */
    public static boolean isFixedRate(final Object task) {

        if (task instanceof Task) {
            final Task ltask = (Task) task;

            return (ltask.relative && ltask.period > 0);
        } else {
            return false;
        }
    }

    /**
     * Retrieves whether the specified argument references a task scheduled
     * periodically using fixed delay scheduling.
     *
     * @param task a task reference
     * @return true if the reference is scheduled using a fixed delay
     */
    public static boolean isFixedDelay(final Object task) {

        if (task instanceof Task) {
            final Task ltask = (Task) task;

            return (!ltask.relative && ltask.period > 0);
        } else {
            return false;
        }
    }

    /**
     * Retrieves whether the specified argument references a task scheduled
     * for periodic execution.
     *
     * @param task a task reference
     * @return true if the task is scheduled for periodic execution
     */
    public static boolean isPeriodic(final Object task) {
        return (task instanceof Task) ? (((Task) task).period > 0)
                                      : false;
    }

    /**
     * Retrieves the last time the referenced task was executed, as a
     * Date object. If the task has never been executed, null is returned.
     *
     * @param task a task reference
     * @return the last time the referenced task was executed; null if never
     */
    public static Date getLastScheduled(Object task) {

        if (task instanceof Task) {
            final Task ltask = (Task) task;
            final long last  = ltask.getLastScheduled();

            return (last == 0) ? null
                               : new Date(last);
        } else {
            return null;
        }
    }

    /**
     * Sets the periodicity of the designated task to a new value. <p>
     *
     * If the designated task is cancelled or the new period is identical to the
     * task's current period, then this invocation has essentially no effect
     * and the submitted object is returned. <p>
     *
     * Otherwise, if the new period is greater than the designated task's
     * current period, then a simple assignment occurs and the submittted
     * object is returned. <p>
     *
     * If neither case holds, then the designated task is cancelled and a new,
     * equivalent task with the new period is scheduled for immediate first
     * execution and returned to the caller. <p>
     *
     * @return a task reference, as per the rules stated above.
     * @param task the task whose periodicity is to be set
     * @param period the new period
     */
    public static Object setPeriod(final Object task, final long period) {
        return (task instanceof Task) ? ((Task) task).setPeriod(period)
                                      : task;
    }

    /**
     * Retrieves the next time the referenced task is due to be executed, as a
     * Date object. If the referenced task is cancelled, null is returned.
     *
     * @param task a task reference
     * @return the next time the referenced task is due to be executed
     */
    public static Date getNextScheduled(Object task) {

        if (task instanceof Task) {
            final Task ltask = (Task) task;
            final long next  = ltask.isCancelled() ? 0
                                                   : ltask.getNextScheduled();

            return next == 0 ? null
                             : new Date(next);
        } else {
            return null;
        }
    }

    /**
     * Adds to the task queue a new Task object encapsulating the supplied
     * Runnable and scheduling arguments.
     *
     * @param first the time of the task's first execution
     * @param runnable the Runnable to execute
     * @param period the task's periodicity
     * @param relative if true, use fixed rate else use fixed delay scheduling
     * @return an opaque reference to the internal task
     */
    protected Task addTask(final long first, final Runnable runnable,
                           final long period, boolean relative) {

        if (this.isShutdown) {
            throw new IllegalStateException("shutdown");
        }

        final Task task = new Task(first, runnable, period, relative);

        // sychronized
        this.taskQueue.addTask(task);

        // sychronized
        this.restart();

        return task;
    }

    /** Sets the background thread to null. */
    protected synchronized void clearThread() {

        try {
            taskRunnerThread.setContextClassLoader(null);
        } catch (Throwable t) {}
        finally {
            taskRunnerThread = null;
        }
    }

    /**
     * Retrieves the next task to execute, or null if this timer is shutdown,
     * the current thread is interrupted, or there are no queued tasks.
     *
     * @return the next task to execute, or null
     */
    protected Task nextTask() {

        try {
            while (!this.isShutdown || Thread.interrupted()) {
                long now;
                long next;
                long wait;
                Task task;

                // synchronized to ensure removeTask
                // applies only to the peeked task,
                // when the computed wait <= 0
                synchronized (this.taskQueue) {
                    task = this.taskQueue.peekTask();

                    if (task == null) {

                        // queue is empty
                        break;
                    }

                    now  = System.currentTimeMillis();
                    next = task.next;
                    wait = (next - now);

                    if (wait > 0) {

                        // release ownership of taskQueue monitor and await
                        // notification of task addition or cancellation,
                        // at most until the time when the peeked task is
                        // next supposed to execute
                        this.taskQueue.park(wait);

                        continue;           // to top of loop
                    } else {
                        this.taskQueue.removeTask();
                    }
                }

                long period = task.period;

                if (period > 0) {           // repeated task
                    if (task.relative) {    // using fixed rate shceduling
                        final long late = (now - next);

                        if (late > period) {

                            // ensure that really late tasks don't
                            // completely saturate the head of the
                            // task queue
                            period = 0;     /** @todo : is -1, -2 ... fairer? */
                        } else if (late > 0) {

                            // compensate for scheduling overruns
                            period -= late;
                        }
                    }

                    task.updateSchedule(now, now + period);
                    this.taskQueue.addTask(task);
                }

                return task;
            }
        } catch (InterruptedException e) {

            //e.printStackTrace();
        }

        return null;
    }

    /**
     * stats var
     */
    static int nowCount = 0;

    /**
     * Convenience method replacing the longer incantation:
     * System.currentTimeMillis()
     *
     * @return System.currentTimeMillis()
     */
    static long now() {

        nowCount++;

        return System.currentTimeMillis();
    }

    /**
     * The Runnable that the background thread uses to execute
     * scheduled tasks. <p>
     *
     * <b>Note:</b> Outer class could simply implement Runnable,
     * but using an inner class protects the public run method
     * from potential abuse.
     */
    protected class TaskRunner implements Runnable {

        /**
         * Runs the next available task in the background thread. <p>
         *
         * When there are no available tasks, the background
         * thread dies and its instance field is cleared until
         * tasks once again become available.
         */
        public void run() {

            try {
                do {
                    final Task task = HsqlTimer.this.nextTask();

                    if (task == null) {
                        break;
                    }

                    // PROBLEM: If the runnable throws an exception other
                    //          than InterruptedException (which likely stems
                    //          naturally from calling shutdownImmediately()
                    //          or getThread().interrupt()), this will still
                    //          cause the loop to exit, which is to say that
                    //          task scheduling will stop until a new task is
                    //          added or the timer is restarted directly, even
                    //          though there may still be uncancelled tasks
                    //          left on the queue.
                    //
                    // TODO:    Clarify and establish a contract regarding
                    //          the difference between InterruptedException,
                    //          RuntimeException and other things, like
                    //          UndeclaredThrowableException.
                    //
                    // SOL'N:   At present, we simply require each runnable to
                    //          understand its part of the implicit contract,
                    //          which is to deal with exceptions internally
                    //          (not throw them up to the timer), with the
                    //          possible exception of InterruptedException.
                    //
                    //          If the integrity of work performed by the
                    //          runnable may be adversely affected by an
                    //          unplanned interruption, the runnable should
                    //          deal with this directly, for instance by
                    //          catching the InterruptedException, ensuring
                    //          that some integrity preserving state is
                    //          attained, and then rethrowing the exception.
                    task.runnable.run();
                } while (true);
            } finally {
                HsqlTimer.this.clearThread();
            }
        }
    }

    /**
     * Encapsulates a Runnable and its scheduling attributes.
     *
     * Essentially, a wrapper class used to schedule a Runnable object
     * for execution by the enclosing HsqlTimer's TaskRunner in a
     * background thread.
     */
    protected class Task {

        /** What to run. */
        Runnable runnable;

        /** The periodic interval, or 0 if one-shot. */
        long period;

        /** The time this task was last executed, or 0 if never. */
        long last;

        /** The next time this task is scheduled to execute. */
        long next;

        /**
         * Whether to silently remove this task instead of running it,
         * the next time (if ever) it makes its way to the head of the
         * timer queue.
         */
        boolean cancelled = false;

        /** Serializes concurrent access to the cancelled field. */
        private Object cancel_mutex = new Object();

        /**
         * Scheduling policy flag. <p>
         *
         * When true, scheduling is fixed rate (as opposed to fixed delay),
         * and schedule updates are calculated relative to when the task was
         * was last run rather than a fixed delay starting from the current
         * wall-clock time provided by System.currentTimeMillis().  <p>
         *
         * This helps normalize scheduling for tasks that must attempt to
         * maintain a fixed rate of execution.
         */
        final boolean relative;

        /**
         * Constructs a new Task object encapulating the specified Runnable
         * and scheduling arguments.
         *
         * @param first the first time to execute
         * @param runnable the Runnable to execute
         * @param period the periodicity of execution
         * @param relative if true, use fixed rate scheduling else fixed delay
         */
        Task(final long first, final Runnable runnable, final long period,
                final boolean relative) {

            this.next     = first;
            this.runnable = runnable;
            this.period   = period;
            this.relative = relative;
        }

        // fixed reported race condition

        /** Sets this task's cancelled flag true and signals its taskQueue. */
        void cancel() {

            boolean signalCancelled = false;

            synchronized (cancel_mutex) {
                if (!cancelled) {
                    cancelled = signalCancelled = true;
                }
            }

            if (signalCancelled) {
                HsqlTimer.this.taskQueue.signalTaskCancelled(this);
            }
        }

        /**
         * Retrieves whether this task is cancelled.
         *
         * @return true if cancelled, else false
         */
        boolean isCancelled() {

            synchronized (cancel_mutex) {
                return cancelled;
            }
        }

        /**
         * Retrieves the instant in time just before this task was
         * last executed by the background thread. A value of zero
         * indicates that this task has never been executed.
         *
         * @return the last time this task was executed or zero if never
         */
        synchronized long getLastScheduled() {
            return last;
        }

        /**
         * Retrieves the time at which this task is next scheduled for
         * execution.
         *
         * @return the time at which this task is next scheduled for
         *      execution
         */
        synchronized long getNextScheduled() {
            return next;
        }

        /**
         * Updates the last and next scheduled execution times.
         *
         * @param last when this task was last executed
         * @param next when this task is to be next executed
         */
        synchronized void updateSchedule(final long last, final long next) {
            this.last = last;
            this.next = next;
        }

        /**
         * Sets the new periodicity of this task in milliseconds. <p>
         *
         * If this task is cancelled or the new period is identical to the
         * current period, then this invocation has essentailly no effect
         * and this object is returned. <p>
         *
         * Otherwise, if the new period is greater than the current period, then
         * a simple field assignment occurs and this object is returned. <p>
         *
         * If none of the previous cases hold, then this task is cancelled and
         * a new, equivalent task with the new period is scheduled for
         * immediate first execution and returned to the caller. <p>
         *
         * @param newPeriod the new period
         * @return a task reference, as per the rules stated above.
         */
        synchronized Object setPeriod(final long newPeriod) {

            if (this.period == newPeriod || this.isCancelled()) {
                return this;
            } else if (newPeriod > this.period) {
                this.period = newPeriod;

                return this;
            } else {
                this.cancel();

                return HsqlTimer.this.addTask(now(), this.runnable, newPeriod,
                                              this.relative);
            }
        }
    }

    /**
     * Heap-based priority queue.
     *
     * Provides extensions to facilitate and simplify implementing
     * timer functionality.
     */
    protected static class TaskQueue extends HsqlArrayHeap {

        /**
         * Constructs a new TaskQueue with the specified initial capacity and
         * ObjectComparator.
         *
         * @param capacity the initial capacity of the queue
         * @param oc The ObjectComparator this queue uses to maintain its
         *      Heap invariant.
         */
        TaskQueue(final int capacity, final ObjectComparator oc) {
            super(capacity, oc);
        }

        /**
         * Type-safe add method. <p>
         *
         * Can be used to inject debugging or accounting behaviour. <p>
         *
         * @param task the task to add
         */
        void addTask(final Task task) {

            // System.out.println("task added: " + task);
            super.add(task);
        }

        /**
         * Atomically removes all tasks in this queue and then and cancels
         * them.
         */
        void cancelAllTasks() {

            Object[] oldHeap;
            int      oldCount;

            synchronized (this) {
                oldHeap  = this.heap;
                oldCount = this.count;

                // 1 instead of 0 to avoid unintended aoob exceptions
                this.heap  = new Object[1];
                this.count = 0;
            }

            for (int i = 0; i < oldCount; i++) {
                ((Task) oldHeap[i]).cancelled = true;
            }
        }

        /**
         * Causes the calling thread to wait until another thread invokes
         * {@link #unpark() unpark} or the specified amount of time has
         * elapsed.
         *
         * Implements the sync & wait(n) half of this queue's availability
         * condition. <p>
         *
         * @param timeout the maximum time to wait in milliseconds.
         * @throws java.lang.InterruptedException if another thread has
         *    interrupted the current thread.  The <i>interrupted status</i> of
         *    the current thread is cleared when this exception is thrown.
         */
        synchronized void park(final long timeout)
        throws InterruptedException {
            this.wait(timeout);
        }

        /**
         * Retrieves the head of this queue, without removing it. <p>
         *
         * This method has the side-effect of removing tasks from the
         * head of this queue until a non-cancelled task is encountered
         * or this queue is empty. <p>
         *
         * If this queue is initially empty or is emptied in the process
         * of finding the earliest scheduled non-cancelled task,
         * then null is returned. <p>
         *
         * @return the earliest scheduled non-cancelled task, or null if no such
         *      task exists
         */
        synchronized Task peekTask() {

            while (super.heap[0] != null
                    && ((Task) super.heap[0]).isCancelled()) {
                super.remove();
            }

            return (Task) super.heap[0];
        }

        /**
         * Informs this queue that the given task is supposedly cancelled. <p>
         *
         * If the indicated task is identical to the current head of
         * this queue, then it is removed and this queue is
         * {@link #unpark() unparked}. <p>
         *
         * The cancelled status of the given task is not verified; it is
         * assumed that the caller is well-behaved (always passes a
         * non-null reference to a cancelled task).
         *
         * @param task a supposedly cancelled task
         */
        synchronized void signalTaskCancelled(Task task) {

            // We only care about the case where HsqlTimer.nextTask
            // might be parked momentarily on this task.
            if (task == super.heap[0]) {
                super.remove();
                this.notify();
            }
        }

        /**
         * Type-safe remove method. <p>
         *
         * Removes the head task from this queue. <p>
         *
         * Can be used to inject debugging or accounting behaviour. <p>
         *
         * @return this queue's head task or null if no such task exists
         */
        Task removeTask() {

            // System.out.println("removing task...");
            return (Task) super.remove();
        }

        /**
         * Wakes up a single thread (if any) that is waiting on this queue's
         * {@link #park(long) park} method.
         *
         * Implements the sync & notify half of this queue's availability
         * condition.
         */
        synchronized void unpark() {
            this.notify();
        }
    }
}
