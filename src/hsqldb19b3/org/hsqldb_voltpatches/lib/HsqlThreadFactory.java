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

/**
 * The default HSQLDB thread factory implementation.  This factory can be
 * used to wrap other thread factories using the setImpl method, but, by
 * default simply produces new, vanilla thread objects constructed with
 * the supplied runnable object.
 *
 * @author boucherb@users
 * @version 1.7.2
 * @since 1.7.2
 */
class HsqlThreadFactory implements ThreadFactory {

    /**
     * The factory implementation.  Typically, this will be the
     * HsqlThreadFactory object itself.
     */
    protected ThreadFactory factory;

    /**
     * Constructs a new HsqlThreadFactory that uses itself as the factory
     * implementation.
     */
    public HsqlThreadFactory() {
        this(null);
    }

    /**
     * Constructs a new HsqlThreadFactory whose retrieved threads come from the
     * specified ThreadFactory object or from this factory implementation, if'
     * the specified implementation is null.
     *
     * @param f the factory implementation this factory uses
     */
    public HsqlThreadFactory(ThreadFactory f) {
        setImpl(f);
    }

    /**
     * Retreives a thread instance for running the specified Runnable
     * @param r The runnable that the retrieved thread handles
     * @return the requested thread inatance
     */
    public Thread newThread(Runnable r) {
        return factory == this ? new Thread(r)
                               : factory.newThread(r);
    }

    /**
     * Sets the factory implementation that this factory will use to
     * produce threads.  If the specified argument, f, is null, then
     * this factory uses itself as the implementation.
     *
     * @param f the factory implementation that this factory will use
     *      to produce threads
     * @return the previously installed factory implementation
     */
    public synchronized ThreadFactory setImpl(ThreadFactory f) {

        ThreadFactory old;

        old     = factory;
        factory = (f == null) ? this
                              : f;

        return old;
    }

    /**
     * Retrieves the factory implementation that this factory is using
     * to produce threads.
     *
     * @return the factory implementation that this factory is using to produce
     * threads.
     */
    public synchronized ThreadFactory getImpl() {
        return factory;
    }
}
