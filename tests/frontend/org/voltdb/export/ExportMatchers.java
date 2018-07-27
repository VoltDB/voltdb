/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.export;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.common.Constants;

import com.google_voltpatches.common.base.Preconditions;

public class ExportMatchers {

    public static final Matcher<VoltMessage> ackMbxMessageIs(
            final int partitionId,
            final String signature,
            final long uso) {
        return new TypeSafeMatcher<VoltMessage>() {
            Matcher<BinaryPayloadMessage> payloadMatcher =
                    ackPayloadIs(partitionId, signature, uso);
            @Override
            public void describeTo(Description d) {
                d.appendDescriptionOf(payloadMatcher);
            }
            @Override
            protected boolean matchesSafely(VoltMessage m) {
                return instanceOf(BinaryPayloadMessage.class).matches(m)
                        && payloadMatcher.matches(BinaryPayloadMessage.class.cast(m));
            }
        };
    }
    public static final Matcher<BinaryPayloadMessage> ackPayloadIs(
            final int partitionId,
            final String signature,
            final long uso) {

        return new TypeSafeMatcher<BinaryPayloadMessage>() {
            Matcher<AckPayloadMessage> payloadMatcher =
                    ackMessageIs(partitionId, signature, uso);
            @Override
            public void describeTo(Description d) {
                d.appendText("BinaryPayloadMessage [ partitionId: ")
                .appendValue(partitionId).appendText(", uso: ")
                .appendValue(uso).appendText(", signature: ")
                .appendValue(signature).appendText("]");
            }
            @Override
            protected boolean matchesSafely(BinaryPayloadMessage p) {
                AckPayloadMessage msg;
                try {
                    msg = new AckPayloadMessage(p);
                }
                catch( BufferUnderflowException buex) {
                    return false;
                }
                return payloadMatcher.matches(msg);
            }
        };
    }

    static final Matcher<AckPayloadMessage> ackMessageIs(
            final int partitionId,
            final String signature,
            final long uso) {

        return new TypeSafeMatcher<AckPayloadMessage>() {
            @Override
            public void describeTo(Description d) {
                d.appendText("AckPayloadMessage [ partitionId: ")
                .appendValue(partitionId).appendText(", uso: ")
                .appendValue(uso).appendText(", signature: ")
                .appendValue(signature).appendText("]");
            }
            @Override
            protected boolean matchesSafely(AckPayloadMessage m) {
                return equalTo(partitionId).matches(m.getPartitionId())
                        && equalTo(signature).matches(m.getSignature())
                        && equalTo(uso).matches(m.getUso());
            }
        };
    }

    static class AckPayloadMessage {
        int partitionId;
        String signature;
        long uso;

        AckPayloadMessage(BinaryPayloadMessage p) {
            ByteBuffer buf = ByteBuffer.wrap(p.m_payload);
            buf.get(); // skip message type

            partitionId = buf.getInt();

            int pSignatureLen = buf.getInt();
            byte [] pSignatureBytes = new byte[pSignatureLen];
            buf.get(pSignatureBytes);
            signature = new String( pSignatureBytes, Constants.UTF8ENCODING);

            uso = buf.getLong();
        }

        AckPayloadMessage(int partitionId, String signature, long uso) {
            Preconditions.checkArgument(signature != null && ! signature.trim().isEmpty());
            Preconditions.checkArgument(uso >= 0);

            this.partitionId = partitionId;
            this.signature = signature;
            this.uso = uso;
        }

        int getPartitionId() {
            return partitionId;
        }

        String getSignature() {
            return signature;
        }

        long getUso() {
            return uso;
        }

        VoltMessage asVoltMessage() {
            byte [] signatureBytes = signature.getBytes(Constants.UTF8ENCODING);
            ByteBuffer buf = ByteBuffer.allocate(17 + signatureBytes.length);
            buf.put((byte)0);
            buf.putInt(partitionId);
            buf.putInt(signatureBytes.length);
            buf.put(signatureBytes);
            buf.putLong(uso);

            return new BinaryPayloadMessage(new byte[0], buf.array());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + partitionId;
            result = prime * result
                    + ((signature == null) ? 0 : signature.hashCode());
            result = prime * result + (int) (uso ^ (uso >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AckPayloadMessage other = (AckPayloadMessage) obj;
            if (partitionId != other.partitionId)
                return false;
            if (signature == null) {
                if (other.signature != null)
                    return false;
            } else if (!signature.equals(other.signature))
                return false;
            if (uso != other.uso)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "AckPayloadMessage [partitionId=" + partitionId
                    + ", signature=" + signature + ", uso=" + uso + "]";
        }
    }
}
