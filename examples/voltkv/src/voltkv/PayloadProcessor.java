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
package voltkv;

import java.util.Arrays;
import java.util.Random;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;

public class PayloadProcessor
{
    public static class Pair
    {
        public final String Key;
        private final byte[] RawValue;
        private final byte[] StoreValue;
        protected Pair(String key, byte[] rawValue, byte[] storeValue)
        {
            this.Key = key;
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

    private final byte[] ValueBase;
    private final int KeySize;
    private final int MinValueSize;
    private final int MaxValueSize;
    private final int PoolSize;
    private final boolean UseCompression;
    public final String KeyFormat;
    private final Random Rand = new Random();
    public PayloadProcessor(int keySize, int minValueSize, int maxValueSize, int poolSize, boolean useCompression)
    {
        this.KeySize = keySize;
        this.MinValueSize = minValueSize;
        this.MaxValueSize = maxValueSize;
        this.PoolSize = poolSize;
        this.UseCompression = useCompression;
        this.ValueBase = new byte[this.MaxValueSize];

        // Set the "64" to whatever number of "values" from 256 you want included in the payload, the lower the number the more compressible the payload
        for (int i=0; i < this.MaxValueSize; i++)
            this.ValueBase[i] = (byte)this.Rand.nextInt(64);

        // Get the base key format string used to generate keys
        this.KeyFormat = "K%1$#" + (this.KeySize-1) + "s";
    }

    public Pair generateForStore()
    {
        final String key = String.format(this.KeyFormat, this.Rand.nextInt(this.PoolSize));
        final byte[] rawValue = Arrays.copyOfRange(this.ValueBase,0,this.MinValueSize+this.Rand.nextInt(this.MaxValueSize-this.MinValueSize+1));
        if (this.UseCompression)
            return new Pair(key, rawValue, gzip(rawValue));
        else
            return new Pair(key, rawValue, null);
    }

    public String generateRandomKeyForRetrieval()
    {
        return String.format(this.KeyFormat, this.Rand.nextInt(this.PoolSize));
    }

    public Pair retrieveFromStore(String key, byte[] storeValue)
    {
        if (this.UseCompression)
            return new Pair(key, gunzip(storeValue), storeValue);
        else
            return new Pair(key, storeValue, null);
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
