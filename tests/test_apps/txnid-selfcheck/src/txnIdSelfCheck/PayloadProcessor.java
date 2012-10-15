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
package txnIdSelfCheck;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import java.nio.ByteBuffer;

public class PayloadProcessor
{
    public static class Pair
    {
        private final byte[] RawValue;
        private final byte[] StoreValue;
        protected Pair(byte[] rawValue, byte[] storeValue)
        {
            this.RawValue = rawValue;
            this.StoreValue = storeValue;
        }
        public long getStoreValueLength()
        {
            if (this.StoreValue == null)
                return this.RawValue.length;
            return this.StoreValue.length;
        }
        public long getRawValueLength()
        {
            return this.RawValue.length;
        }
        public byte[] getStoreValue()
        {
            if (this.StoreValue == null)
                return this.RawValue;
            return this.StoreValue;
        }
        public byte[] getRawValue()
        {
            return this.RawValue;
        }
    }

    private final int MinValueSize;
    private final int MaxValueSize;
    private final boolean UseCompression;
    private final int Entropy;
    private final Random Rand = new Random(0);

    /*
     * Volt deals with 2 megs at the most so 4 megabytes of entropy is plenty
     */
    private final ByteBuffer entropyBytes = ByteBuffer.allocate (1024 * 1024 * 4);

    public PayloadProcessor(
            int minValueSize,
            int maxValueSize,
            int entropy,
            boolean useCompression)
    {
        this.MinValueSize = minValueSize;
        this.MaxValueSize = maxValueSize;
        this.UseCompression = useCompression;
        this.Entropy = entropy;
        if (entropy < 1 || entropy > 127) {
            throw new IllegalArgumentException("Entropy must be a number between 1 and 127");
        }
        while (entropyBytes.hasRemaining()) {
            entropyBytes.put((byte)(Rand.nextInt(127) % Entropy));
        }
    }

    public Pair generateForStore()
    {
        final byte[] rawValue = new byte[this.MinValueSize+this.Rand.nextInt(this.MaxValueSize-this.MinValueSize+1)];
        if (entropyBytes.remaining() > rawValue.length){
            entropyBytes.get(rawValue);
        } else {
            entropyBytes.position(0);
            entropyBytes.get(rawValue);
        }
        if (this.UseCompression)
            return new Pair(rawValue, gzip(rawValue));
        else
            return new Pair(rawValue, null);
    }

    public Pair retrieveFromStore(byte[] storeValue)
    {
        if (this.UseCompression)
            return new Pair(gunzip(storeValue), storeValue);
        else
            return new Pair(storeValue, null);
    }

    private static byte[] gzip(byte[] bytes)
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int)(bytes.length * 0.7));
            GZIPOutputStream gzos = new GZIPOutputStream(baos);
            gzos.write(bytes);
            gzos.close();
            return baos.toByteArray();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    private static byte[] gunzip(byte bytes[])
    {
        // Check to see if it's gzip-compressed
        // GZIP Magic Two-Byte Number: 0x8b1f (35615)
        if( (bytes != null) && (bytes.length >= 4) )
        {

            int head = (bytes[0] & 0xff) | ((bytes[1] << 8) & 0xff00);
            if( GZIPInputStream.GZIP_MAGIC == head )
            {
                ByteArrayInputStream  bais = null;
                GZIPInputStream gzis = null;
                ByteArrayOutputStream baos = null;
                byte[] buffer = new byte[2048];
                int    length = 0;

                try
                {
                    baos = new ByteArrayOutputStream();
                    bais = new ByteArrayInputStream( bytes );
                    gzis = new GZIPInputStream( bais );

                    while( ( length = gzis.read( buffer ) ) >= 0 )
                        baos.write(buffer,0,length);

                    // No error? Get new bytes.
                    bytes = baos.toByteArray();

                }   // end try
                catch( java.io.IOException e )
                {
                    e.printStackTrace();
                    // Just return originally-decoded bytes
                }   // end catch
                finally
                {
                    try{ baos.close(); } catch( Exception e ){}
                    try{ gzis.close(); } catch( Exception e ){}
                    try{ bais.close(); } catch( Exception e ){}
                }   // end finally

            }   // end if: gzipped
        }   // end if: bytes.length >= 2
        return bytes;
    }
}
