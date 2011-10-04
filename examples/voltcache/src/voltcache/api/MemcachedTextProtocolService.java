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

import java.io.BufferedOutputStream;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.nio.charset.Charset;

public class MemcachedTextProtocolService implements Runnable
{
    // Connection Id generator
    private static long IdGenerator = 0l;

    // Base error messages
    public static final String INVALID_ARGUMENT_COUNT   = "Invalid argument count (%d) for this operation.";
    public static final String INVALID_ARGUMENT_FORMAT  = "Invalid argument %d format (value: %s): expected %s.";
    public static final String INVALID_NOREPLY_ARGUMENT = "Invalid 'noreply' argument: received '%s' instead.";
    public static final String PREPARATION_EXCEPTION    = "Exception while preparing request: %s";
    public static final String EXECUTION_EXCEPTION    = "Exception while executing request: %s";

    // Preserialized Responses and Response Fragments
    private static final byte[] RESPONSE_STORED       = getResponseBytes(VoltCacheResult.getName(VoltCacheResult.STORED) + "\r\n");
    private static final byte[] RESPONSE_NOT_STORED   = getResponseBytes(VoltCacheResult.getName(VoltCacheResult.NOT_STORED) + "\r\n");
    private static final byte[] RESPONSE_EXISTS       = getResponseBytes(VoltCacheResult.getName(VoltCacheResult.EXISTS) + "\r\n");
    private static final byte[] RESPONSE_NOT_FOUND    = getResponseBytes(VoltCacheResult.getName(VoltCacheResult.NOT_FOUND) + "\r\n");
    private static final byte[] RESPONSE_DELETED      = getResponseBytes(VoltCacheResult.getName(VoltCacheResult.DELETED) + "\r\n");
    private static final byte[] RESPONSE_ERROR        = getResponseBytes(VoltCacheResult.getName(VoltCacheResult.ERROR) + "\r\n");
    private static final byte[] RESPONSE_CLIENT_ERROR = getResponseBytes(VoltCacheResult.getName(VoltCacheResult.CLIENT_ERROR) + "\r\n");
    private static final byte[] RESPONSE_SERVER_ERROR = getResponseBytes(VoltCacheResult.getName(VoltCacheResult.SERVER_ERROR) + "\r\n");
    private static final byte[] RESPONSE_OK           = getResponseBytes(VoltCacheResult.getName(VoltCacheResult.OK) + "\r\n");
    private static final byte[] RESPONSE_SUBMITTED    = getResponseBytes(VoltCacheResult.getName(VoltCacheResult.SUBMITTED) + "\r\n");
    private static final byte[][] RESPONSES = new byte[][] { RESPONSE_STORED, RESPONSE_NOT_STORED, RESPONSE_EXISTS, RESPONSE_NOT_FOUND, RESPONSE_DELETED, RESPONSE_ERROR, RESPONSE_CLIENT_ERROR, RESPONSE_SERVER_ERROR, RESPONSE_OK, RESPONSE_SUBMITTED };

    private static byte[] RESPONSE_VALUE = getResponseBytes("VALUE ");
    private static int RESPONSE_VALUE_Length = RESPONSE_VALUE.length;
    private static final byte[] OneSpace = getResponseBytes(" ");
    private static final int OneSpace_Length = OneSpace.length;
    private static final byte[] NewLine = getResponseBytes("\r\n");
    private static final int NewLine_Length = NewLine.length;
    private static final byte[] EndLine = getResponseBytes("END\r\n");
    private static final int EndLine_Length = EndLine.length;

    // Commands
    private static final String COMMAND_ADD      = "add";
    private static final String COMMAND_APPEND   = "append";
    private static final String COMMAND_PREPEND  = "prepend";
    private static final String COMMAND_REPLACE  = "replace";
    private static final String COMMAND_SET      = "set";
    private static final String COMMAND_CAS      = "cas";
    private static final String COMMAND_DELETE   = "delete";
    private static final String COMMAND_FLUSHALL = "flush_all";
    private static final String COMMAND_GET      = "get";
    private static final String COMMAND_GETS     = "gets";
    private static final String COMMAND_VERSION  = "version";
    private static final String COMMAND_INCR     = "incr";
    private static final String COMMAND_DECR     = "decr";
    private static final String COMMAND_QUIT     = "quit";

    // Parsing helpers
    private static final String NOREPLY = "noreply";
    private static final Pattern splitter = Pattern.compile(" ");
    public static final Charset USASCII = Charset.forName("US-ASCII");

    // Instance members
    private final long Id;
    private final Socket Socket;
    private final InputStream in;
//    private final BufferedInputStream in;
    private final BufferedOutputStream out;
    private final VoltCache Cache;
    private boolean Active = false;


    public MemcachedTextProtocolService(Socket socket, String voltcacheServerList, int voltcachePort) throws Exception
    {
        this.Id     = IdGenerator++;
        this.Socket = socket;
        this.in     = socket.getInputStream();
//        this.in     = new BufferedInputStream(socket.getInputStream(), 65535);
        this.out    = new BufferedOutputStream(socket.getOutputStream(), 65535);
        this.Cache  = new VoltCache(voltcacheServerList, voltcachePort);
        System.out.println("Opening Client Connection[" + this.Id + "] :: " + this.Socket.getInetAddress());
    }

    @Override
    public void run()
    {
        this.Active = true;
        while(this.Active)
        {
            try
            {
                this.answer();
            }
            catch(Exception x)
            {
                this.kill();
            }
        }
    }

    protected boolean kill()
    {
        this.Active = false;
        try { System.out.println("Closing Client Connection[" + this.Id + "] :: " + this.Socket.getInetAddress()); } catch(Exception x) {}
        try { this.Cache.close(); } catch(Exception x) {}
        try { this.in.close(); } catch(Exception x) {}
        try { this.out.close(); } catch(Exception x) {}
        try { this.Socket.close(); } catch(Exception x) {}
        return false;
    }

    private final byte[] msg = new byte[2048];
    protected void answer() throws Exception
    {
        boolean gotR = false;
        int b;
        int xi = 0;

        int tr = this.in.read(msg);
        if (tr == -1)
            throw new IOException("Stream terminated");

        while(true)
        {
            b = msg[xi];
            if (xi >= tr)
                throw new IOException("Stream terminated");
            if (b == 13)
            {
                gotR = true;
                xi++;
                continue;
            }
            if (gotR)
            {
                if (b == 10)
                {
                    xi++;
                    break;
                }
                msg[xi++] = 13;
                gotR = false;
            }
            xi++;
        }
        // Parse first line arguments to figure out command
        final String[] args  = splitter.split(new String(msg, 0, xi, USASCII).trim());
        final String command = args[0].toLowerCase();
        final int argCount   = args.length-1;

        // GET
        if (command.equals(COMMAND_GET) || command.equals(COMMAND_GETS))
        {
            if (argCount == 0)
                this.replyClientError(INVALID_ARGUMENT_COUNT, argCount);
            else if (argCount == 1)
            {
                try
                {
                    this.replyData(args[1], this.Cache.get(args[1]));
                }
                catch(Exception e)
                {
                    this.replyServerError(EXECUTION_EXCEPTION, e);
                }
            }
            else
            {
                try
                {
                    this.replyData(this.Cache.get(Arrays.copyOfRange(args, 1, args.length-1)));
                }
                catch(Exception e)
                {
                    this.replyServerError(EXECUTION_EXCEPTION, e);
                }
            }
        }

        // ADD, APPEND, PREPEND, REPLACE, SET
        else if (
                command.equals(COMMAND_SET)
             || command.equals(COMMAND_ADD)
             || command.equals(COMMAND_APPEND)
             || command.equals(COMMAND_PREPEND)
             || command.equals(COMMAND_REPLACE)
           )
        {
            if ((argCount < 4) || (argCount > 5))
                this.replyClientError(INVALID_ARGUMENT_COUNT, argCount);
            else
                try
                {
                    final String key = args[1];
                    final int flags = Integer.valueOf(args[2]);
                    final int exptime = Integer.valueOf(args[3]);
                    final int byteCount = Integer.valueOf(args[4]);
                    final byte[] data = new byte[byteCount];
                    int avail = tr-xi;
                    if (avail < byteCount)
                    {
                        System.arraycopy(msg, xi, data, 0, avail);
                        this.in.read(data, avail, byteCount-avail);
                        this.in.skip(2);
                    }
                    else
                    {
                        System.arraycopy(msg, xi, data, 0, byteCount);
                        if (byteCount+2 > avail)
                            this.in.skip(byteCount+2-avail);
                    }
                    boolean noreply = false;
                    if (argCount == 5)
                    {
                        if (args[5].equals(NOREPLY))
                            noreply = true;
                        else
                        {
                            this.replyClientError(INVALID_NOREPLY_ARGUMENT, args[5]);
                            return;
                        }
                    }

                    try
                    {
                        // Run actual operation
                        if(command.equals(COMMAND_SET))
                            this.replyStatus(this.Cache.set(key, flags, exptime, data, noreply));
                        else if(command.equals(COMMAND_ADD))
                            this.replyStatus(this.Cache.add(key, flags, exptime, data, noreply));
                        else if(command.equals(COMMAND_APPEND))
                            this.replyStatus(this.Cache.append(key, data, noreply));
                        else if(command.equals(COMMAND_PREPEND))
                            this.replyStatus(this.Cache.prepend(key, data, noreply));
                        else if(command.equals(COMMAND_REPLACE))
                            this.replyStatus(this.Cache.replace(key, flags, exptime, data, noreply));
                    }
                    catch(Exception e)
                    {
                        this.replyServerError(EXECUTION_EXCEPTION, e);
                    }
                }
                catch(Exception x)
                {
                    this.replyClientError(PREPARATION_EXCEPTION, x);
                }
        }

        // CAS
        else if (command.equals(COMMAND_CAS))
        {
            if ((argCount < 5) || (argCount > 6))
                this.replyClientError(INVALID_ARGUMENT_COUNT, argCount);
            else
                try
                {
                    final String key = args[1];
                    final int flags = Integer.valueOf(args[2]);
                    final int exptime = Integer.valueOf(args[3]);
                    final int byteCount = Integer.valueOf(args[4]);
                    final byte[] data = new byte[byteCount];
                    int avail = tr-xi;
                    if (avail < byteCount)
                    {
                        System.arraycopy(msg, xi, data, 0, avail);
                        this.in.read(data, avail, byteCount-avail);
                        this.in.skip(2);
                    }
                    else
                    {
                        System.arraycopy(msg, xi, data, 0, byteCount);
                        if (byteCount+2 > avail)
                            this.in.skip(byteCount+2-avail);
                    }
                    final long casVersion = Long.valueOf(args[5]);
                    boolean noreply = false;
                    if (argCount == 6)
                    {
                        if (args[6].equals(NOREPLY))
                            noreply = true;
                        else
                        {
                            this.replyClientError(INVALID_NOREPLY_ARGUMENT, args[6]);
                            return;
                        }
                    }

                    // Run actual operation
                    try
                    {
                        this.replyStatus(this.Cache.cas(key, flags, exptime, data, casVersion, noreply));
                    }
                    catch(Exception e)
                    {
                        this.replyServerError(EXECUTION_EXCEPTION, e);
                    }
                }
                catch(Exception x)
                {
                    this.replyClientError(PREPARATION_EXCEPTION, x);
                }
        }

        // DELETE
        else if (command.equals(COMMAND_DELETE))
        {
            if (argCount == 0 || argCount > 3)
                this.replyClientError(INVALID_ARGUMENT_COUNT, argCount);
            else
            {
                try
                {
                    final String key = args[1];
                    int exptime = 0;
                    boolean noreply = false;
                    if (argCount == 2)
                    {
                        if (args[2].equals(NOREPLY))
                            noreply = true;
                        else
                            exptime = Integer.valueOf(args[2]);
                    }
                    else if (argCount == 3)
                    {
                        exptime = Integer.valueOf(args[1]);
                        if (args[3].equals(NOREPLY))
                            noreply = true;
                        else
                        {
                            this.replyClientError(INVALID_NOREPLY_ARGUMENT, args[3]);
                            return;
                        }
                    }

                    // Run actual operation
                    try
                    {
                        this.replyStatus(this.Cache.delete(key, exptime, noreply));
                    }
                    catch(Exception e)
                    {
                        this.replyServerError(EXECUTION_EXCEPTION, e);
                    }
                }
                catch(Exception x)
                {
                    this.replyClientError(PREPARATION_EXCEPTION, x);
                }
            }
        }

        // FLUSH_ALL
        else if (command.equals(COMMAND_FLUSHALL))
        {
            if (argCount > 2)
                this.replyClientError(INVALID_ARGUMENT_COUNT, argCount);
            else
            {
                try
                {
                    int exptime = 0;
                    boolean noreply = false;
                    if (argCount == 1)
                    {
                        if (args[1].equals(NOREPLY))
                            noreply = true;
                        else
                            exptime = Integer.valueOf(args[1]);
                    }
                    else if (argCount == 2)
                    {
                        exptime = Integer.valueOf(args[1]);
                        if (args[2].equals(NOREPLY))
                            noreply = true;
                        else
                        {
                            this.replyClientError(INVALID_NOREPLY_ARGUMENT, args[2]);
                            return;
                        }
                    }

                    // Run actual operation
                    try
                    {
                        this.replyStatus(this.Cache.flushAll(exptime, noreply));
                    }
                    catch(Exception e)
                    {
                        this.replyServerError(EXECUTION_EXCEPTION, e);
                    }
                }
                catch(Exception x)
                {
                    this.replyClientError(PREPARATION_EXCEPTION, x);
                }
            }
        }

        // VERSION
        else if (command.equals(COMMAND_VERSION))
        {
            this.reply("VERSION 1.4.5\r\n"); // Pretend to be Memcached server 1.4.5
        }

        // INCR, DECR
        else if (command.equals(COMMAND_INCR) || command.equals(COMMAND_DECR))
        {
            if (argCount < 2 || argCount > 3)
                this.replyClientError(INVALID_ARGUMENT_COUNT, argCount);
            else
            {
                try
                {
                    final String key = args[1];
                    final long by = Long.valueOf(args[2]);
                    boolean noreply = false;
                    if (argCount == 3)
                    {
                        if (args[3].equals(NOREPLY))
                            noreply = true;
                        else
                        {
                            this.replyClientError(INVALID_NOREPLY_ARGUMENT, args[3]);
                            return;
                        }
                    }

                    // Run actual operation
                    try
                    {
                        this.replyIncrDecr(this.Cache.incrDecr(key, by, command.equals(COMMAND_INCR), noreply), noreply);
                    }
                    catch(Exception e)
                    {
                        this.replyServerError(EXECUTION_EXCEPTION, e);
                    }
                }
                catch(Exception x)
                {
                    this.replyClientError(PREPARATION_EXCEPTION, x);
                }
            }
        }
        else if (command.equals(COMMAND_QUIT))
            this.kill();
        else
            this.replyError();
    }

    private static byte[] getResponseBytes(String responseString)
    {
        try
        {
            return USASCII.encode(responseString).array(); //responseString.getBytes(USASCII);
        }
        catch(Exception x)
        {
            return responseString.getBytes();
        }
    }

    private void reply(byte[] response) throws Exception
    {
        this.out.write(response, 0, response.length);
        this.out.flush();
    }

    private void reply(String responseString) throws Exception
    {
        this.reply(getResponseBytes(responseString));
    }

    private void replyError() throws Exception
    {
        this.reply(RESPONSE_ERROR);
    }

    private void replyClientError(String format, Object... parameters) throws Exception
    {
        this.reply("CLIENT_ERROR " + String.format(format, parameters).replaceAll("\r\n","\n") + "\r\n");
    }

    private void replyServerError(String format, Object... parameters) throws Exception
    {
        this.reply("SERVER_ERROR " + String.format(format, parameters).replaceAll("\r\n","\n") + "\r\n");
    }

    private void replyData(VoltCacheResult response) throws Exception
    {
        if (response.Data != null)
        {
            Iterator<VoltCacheItem> itr = (Iterator<VoltCacheItem>)((Collection<VoltCacheItem>)response.Data.values()).iterator();
            while (itr.hasNext())
            {
                final VoltCacheItem item = itr.next();
                byte[] value;

                this.out.write(RESPONSE_VALUE, 0, RESPONSE_VALUE_Length);

                value = getResponseBytes(item.Key);
                this.out.write(value, 0, value.length);

                this.out.write(OneSpace, 0, OneSpace_Length);

                value = getResponseBytes(Integer.toString(item.Flags));
                this.out.write(value, 0, value.length);

                this.out.write(OneSpace, 0, OneSpace_Length);

                value = getResponseBytes(Integer.toString(item.Value.length));
                this.out.write(value, 0, value.length);

                this.out.write(OneSpace, 0, OneSpace_Length);

                value = getResponseBytes(Long.toString(item.CASVersion));
                this.out.write(value, 0, value.length);

                this.out.write(NewLine, 0, NewLine_Length);

                this.out.write(item.Value, 0, item.Value.length);
                this.out.write(NewLine, 0, NewLine_Length);
            }
        }
        this.out.write(EndLine, 0, EndLine.length);
        this.out.flush();
    }

    private void replyData(String key, VoltCacheResult response) throws Exception
    {
        if (response.Data != null)
        {
            if (response.Data.containsKey(key))
            {
                final VoltCacheItem item = response.Data.get(key);

                byte[] value;

                this.out.write(RESPONSE_VALUE, 0, RESPONSE_VALUE_Length);

                value = getResponseBytes(key);
                this.out.write(value, 0, value.length);

                this.out.write(OneSpace, 0, OneSpace_Length);

                value = getResponseBytes(Integer.toString(item.Flags));
                this.out.write(value, 0, value.length);

                this.out.write(OneSpace, 0, OneSpace_Length);

                value = getResponseBytes(Integer.toString(item.Value.length));
                this.out.write(value, 0, value.length);

                this.out.write(OneSpace, 0, OneSpace_Length);

                value = getResponseBytes(Long.toString(item.CASVersion));
                this.out.write(value, 0, value.length);

                this.out.write(NewLine, 0, NewLine_Length);

                this.out.write(item.Value, 0, item.Value.length);
                this.out.write(NewLine, 0, NewLine_Length);
            }
        }
        this.out.write(EndLine, 0, EndLine_Length);
        this.out.flush();

    }

    private void replyIncrDecr(VoltCacheResult response, boolean noreply) throws Exception
    {
        if (noreply)
            this.replyStatus(response);
        else
        {
            if (response.Code == VoltCacheResult.NOT_FOUND)
                this.reply(RESPONSE_NOT_FOUND);
            else
                this.reply(Long.toString(response.IncrDecrValue) + "\r\n");
        }
    }

    private void replyStatus(VoltCacheResult response) throws Exception
    {
        this.reply(RESPONSES[(int)response.Code]);
    }
}
