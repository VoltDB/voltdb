/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package voltcache.api;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.voltdb.client.ClientResponse;

public class VoltCacheFuture implements Future<VoltCacheResult>
{
    protected static VoltCacheFuture cast(VoltCacheResult.Type type, Future<ClientResponse> source)
    {
        return new VoltCacheFuture(type, source);
    }
    protected static VoltCacheFuture fail()
    {
        return new VoltCacheFuture();
    }

    private final Future<ClientResponse> source;
    private final VoltCacheResult.Type type;
    private VoltCacheFuture(VoltCacheResult.Type type, Future<ClientResponse> source)
    {
        this.type = type;
        this.source = source;
    }
    private VoltCacheFuture()
    {
        this.source = null;
        this.type = VoltCacheResult.Type.CODE;
    }
    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        if (this.source == null)
            return false;
        return source.cancel(mayInterruptIfRunning);
    }
    @Override
    public VoltCacheResult get() throws InterruptedException, ExecutionException
    {
        if (this.source == null)
            return VoltCacheResult.ERROR();
        return VoltCacheResult.get(this.type, source.get());
    }
    @Override
    public VoltCacheResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
        if (this.source == null)
            return VoltCacheResult.ERROR();
        return VoltCacheResult.get(this.type, source.get(timeout, unit));
    }
    @Override
    public boolean isCancelled()
    {
        if (this.source == null)
            return false;
        return source.isCancelled();
    }
    @Override
    public boolean isDone()
    {
        if (this.source == null)
            return true;
        return source.isDone();
    }
}
