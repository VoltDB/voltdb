/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.utils;


/**
 * This class implements the Punycode idn ACE encoder
 *  http://www.ietf.org/internet-drafts/draft-ietf-idn-punycode-02.txt
 * it also escapes non e-mail safe characters to the unicode private
 * range of 0xE000 to 0xE0F7
 * @author Stefano Santoro (Java Port of C sample in the ietf draft)
 * @version 1.0
 */


public final class Punycode {

  private static final int punycode_success = 0;
  private static final int punycode_bad_input = 1;
  private static final int punycode_big_output = 2;
  private static final int punycode_overflow = 3;
  private static final int base = 36;
  private static final int tmin = 1;
  private static final int tmax = 26;
  private static final int skew = 38;
  private static final int damp = 700;
  private static final int initial_bias = 72;
  private static final int initial_n = 0x80;
  private static final int delimiter = 0x2D;

  private final static boolean basic( int cp) { return cp < 0x80; }

  private final static boolean delim( int cp) { return cp == delimiter; }

  private final static int decode_digit(char cp)
  {
    return  cp - 48 < 10 ? cp - 22 :  cp - 65 < 26 ? cp - 65 :
            cp - 97 < 26 ? cp - 97 :  base;
  }

  private final static char encode_digit(int d, boolean flag)
  {
    int shifter = (flag ? 1 : 0);
    return (char)(d + 22 + 75 * (d < 26 ? 1 : 0) - (shifter << 5));
    /*  0..25 map to ASCII a..z or A..Z */
    /* 26..35 map to ASCII 0..9         */
  }

  private final static boolean flagged( int bcp) { return bcp - 65 < 26; }

  private final static char encode_basic(int bcp, boolean flag)
  {
    bcp -= ((bcp - 97) < 26 ? 1 : 0) << 5;
    int shifter = ((!flag && ((bcp - 65) < 26)) ? 1 : 0);
    return (char)(bcp + shifter << 5);
  }

  private final static int maxint = Integer.MAX_VALUE;

  private final static int adapt(int delta, int numpoints, boolean firsttime )
  {
    int k;

    delta = firsttime ? delta / damp : delta >>> 1;
    /* delta >> 1 is a faster way of doing delta / 2 */
    delta += delta / numpoints;

    for (k = 0;  delta > ((base - tmin) * tmax) / 2;  k += base) {
      delta /= base - tmin;
    }

    return k + (base - tmin + 1) * delta / (delta + skew);
  }

  private final static class OutputLength {
    int len = 0;
    OutputLength( int aLength) { len = aLength; }
  }

  private final static int punycode_encode(
     final int input[],
     final boolean case_flags[],
     OutputLength output_length,
     char output[] )
  {
    int n, delta, h, b, out, max_out, bias, j, m, q, k, t;

    /* Initialize the state: */

    int input_length = input.length;
    n = initial_n;
    delta = 0;
    out = 0;
    max_out = output_length.len;
    bias = initial_bias;

    /* Handle the basic code points: */

    for (j = 0;  j < input_length;  ++j) {
      if (basic(input[j])) {
        if (max_out - out < 2) return punycode_big_output;
         output[out++] =
         case_flags != null? encode_basic(input[j], case_flags[j]) : (char)input[j];
      }
      /* else if (input[j] < n) return punycode_bad_input; */
      /* (not needed for Punycode with unsigned code points) */
    }

    h = b = out;

    /* h is the number of code points that have been handled, b is the  */
    /* number of basic code points, and out is the number of characters */
    /* that have been output.                                           */

    if (b > 0) output[out++] = delimiter;

    /* Main encoding loop: */

    while (h < input_length) {
      /* All non-basic code points < n have been     */
      /* handled already.  Find the next larger one: */

      for (m = maxint, j = 0;  j < input_length;  ++j) {
        /* if (basic(input[j])) continue; */
        /* (not needed for Punycode) */
        if (input[j] >= n && input[j] < m) m = input[j];
      }

      /* Increase delta enough to advance the decoder's    */
      /* <n,i> state to <m,0>, but guard against overflow: */

      if (m - n > (maxint - delta) / (h + 1)) return punycode_overflow;
      delta += (m - n) * (h + 1);
      n = m;

      for (j = 0;  j < input_length;  ++j) {
        /* Punycode does not need to check whether input[j] is basic: */
        if (input[j] < n /* || basic(input[j]) */ ) {
          if (++delta == 0) return punycode_overflow;
        }

        if (input[j] == n) {
          /* Represent delta as a generalized variable-length integer: */

          for (q = delta, k = base;  ;  k += base) {
            if (out >= max_out) return punycode_big_output;
            t = k <= bias /* + tmin */ ? tmin :     /* +tmin not needed */
                k >= bias + tmax ? tmax : k - bias;
            if (q < t) break;
            output[out++] = encode_digit(t + (q - t) % (base - t), false);
            q = (q - t) / (base - t);
          }

          output[out++] = encode_digit(q, case_flags != null && case_flags[j]);
          bias = adapt(delta, h + 1, h == b);
          delta = 0;
          ++h;
        }
      }

      ++delta;
      ++n;
    }

    output_length.len = out;
    return punycode_success;
  }

  private final static int punycode_decode(
    final char input[],
    OutputLength output_length,
    int output[],
    boolean case_flags[] )
  {
    int n, out, i, max_out, bias,
        b, j, in, oldi, w, k, digit, t;

    /* Initialize the state: */

    int input_length = input.length;
    n = initial_n;
    out = i = 0;
    max_out = output_length.len;
    bias = initial_bias;

    /* Handle the basic code points:  Let b be the number of input code */
    /* points before the last delimiter, or 0 if there is none, then    */
    /* copy the first b code points to the output.                      */

    for (b = j = 0;  j < input_length;  ++j) if (delim(input[j])) b = j;
    if (b > max_out) return punycode_big_output;

    for (j = 0;  j < b;  ++j) {
      if (case_flags != null) case_flags[out] = flagged(input[j]);
      if (!basic(input[j])) return punycode_bad_input;
      output[out++] = input[j];
    }

    /* Main decoding loop:  Start just after the last delimiter if any  */
    /* basic code points were copied; start at the beginning otherwise. */

    for (in = b > 0 ? b + 1 : 0;  in < input_length;  ++out) {

      /* in is the index of the next character to be consumed, and */
      /* out is the number of code points in the output array.     */

      /* Decode a generalized variable-length integer into delta,  */
      /* which gets added to i.  The overflow checking is easier   */
      /* if we increase i as we go, then subtract off its starting */
      /* value at the end to obtain delta.                         */

      for (oldi = i, w = 1, k = base;  ;  k += base) {
        if (in >= input_length) return punycode_bad_input;
        digit = decode_digit(input[in++]);
        if (digit >= base) return punycode_bad_input;
        if (digit > (maxint - i) / w) return punycode_overflow;
        i += digit * w;
        t = (k <= bias /* + tmin */ ? tmin :     /* +tmin not needed */
          (k >= bias + tmax ? tmax : k - bias));
        if (digit < t) break;
        if (w > maxint / (base - t)) return punycode_overflow;
        w *= (base - t);
      }

      bias = adapt(i - oldi, out + 1, oldi == 0);

      /* i was supposed to wrap around from out+1 to 0,   */
      /* incrementing n each time, so we'll fix that now: */

      if (i / (out + 1) > maxint - n) return punycode_overflow;
      n += i / (out + 1);
      i %= (out + 1);

      /* Insert n at position i of the output: */

      /* not needed for Punycode: */
      /* if (decode_digit(n) <= base) return punycode_invalid_input; */
      if (out >= max_out) return punycode_big_output;

      if (case_flags != null) {
        System.arraycopy(case_flags, i, case_flags, i+1, out - i);
        /* Case of last character determines uppercase flag: */
        case_flags[i] = flagged(input[in - 1]);
      }

      System.arraycopy(output, i, output,  i + 1, (out - i) * 4);
      output[i++] = n;
    }

    output_length.len = out;
    return punycode_success;
  }

  private final static char escape_email_unsafe( char c)
    throws IllegalArgumentException
  {
    if( c >= 0xe000 && c < 0xf8ff) {
      throw new IllegalArgumentException
    ("character in unicode private range");
    }
    if( c > 0x7f) return c;
    if( (c>=97 && c<123) || (c>=48 && c<58) || (c>=65&& c<91)) return c;
    if( c == 46 || c == 45 || c == 95) return c;

    return (char)(0xe000 + c);
  }

  private final static char unescape_email_unsafe( char c) {
    if( c>=0xe000 && c<0xf8ff) return (char)(c - 0xe000);
    return c;
  }

  public final static String encode( String aContent)
    throws IllegalArgumentException
  {
    int  [] iInput = new int[aContent.length()];
    char [] cInput = aContent.toCharArray();

    for( int i = 0; i < iInput.length; ++i) {
      iInput[i] = escape_email_unsafe(cInput[i]);
    }

    char [] cOutput = new char[cInput.length << 2];
    OutputLength ol = new OutputLength( cOutput.length);

    int rc = punycode_encode( iInput, null, ol, cOutput);
    if( rc != punycode_success) {
      throw new IllegalArgumentException("could not puny encode: "+rc);
    }
    return new String( cOutput, 0, ol.len);
  }

  public final static String decode( String aContent)
    throws IllegalArgumentException
  {
    int  [] iOutput = new int[ aContent.length() << 2];
    char [] cOutput;
    char [] cInput = aContent.toCharArray();

    OutputLength ol = new OutputLength( iOutput.length);

    int rc = punycode_decode( cInput, ol, iOutput, null);
    if( rc != punycode_success) {
      throw new IllegalArgumentException("could not puny decode: "+rc);
    }

    cOutput = new char[ol.len];
    for( int i = 0; i < ol.len; ++i) {
      cOutput[i] = unescape_email_unsafe((char)iOutput[i]);
    }
    return new String( cOutput);
  }
}
