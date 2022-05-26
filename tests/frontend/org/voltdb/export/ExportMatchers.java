/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
            final long seqNo,
            final long generationId) {
        return new TypeSafeMatcher<VoltMessage>() {
            Matcher<BinaryPayloadMessage> payloadMatcher =
                    ackPayloadIs(partitionId, signature, seqNo, generationId);
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
            final long seqNo,
            final long generationId) {

        return new TypeSafeMatcher<BinaryPayloadMessage>() {
            Matcher<AckPayloadMessage> payloadMatcher =
                    ackMessageIs(partitionId, signature, seqNo, generationId);
            @Override
            public void describeTo(Description d) {
                d.appendText("BinaryPayloadMessage [ partitionId: ")
                .appendValue(partitionId).appendText(", seqNo: ")
                .appendValue(seqNo).appendText(", signature: ")
                .appendValue(signature).appendText(", generationId: ")
                .appendValue(generationId).appendText("]");
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
            final long seqNo,
            final long generationId) {

        return new TypeSafeMatcher<AckPayloadMessage>() {
            @Override
            public void describeTo(Description d) {
                d.appendText("AckPayloadMessage [ partitionId: ")
                .appendValue(partitionId).appendText(", seqNo: ")
                .appendValue(seqNo).appendText(", signature: ")
                .appendValue(signature).appendText(", generationId: ")
                .appendValue(generationId).appendText("]");
            }
            @Override
            protected boolean matchesSafely(AckPayloadMessage m) {
                return equalTo(partitionId).matches(m.getPartitionId())
                        && equalTo(signature).matches(m.getSignature())
                        && equalTo(seqNo).matches(m.getSequenceNumber())
                        && equalTo(generationId).matches((m.getGenerationId()));
            }
        };
    }

    static class AckPayloadMessage {
        int partitionId;
        String signature;
        long seqNo;
        long generationId;

        AckPayloadMessage(BinaryPayloadMessage p) {
            ByteBuffer buf = ByteBuffer.wrap(p.m_payload);
            buf.get(); // skip message type

            partitionId = buf.getInt();

            int pSignatureLen = buf.getInt();
            byte [] pSignatureBytes = new byte[pSignatureLen];
            buf.get(pSignatureBytes);
            signature = new String( pSignatureBytes, Constants.UTF8ENCODING);

            seqNo = buf.getLong();
            generationId = buf.getLong();
        }

        AckPayloadMessage(int partitionId, String signature, long seqNo, long generationId) {
            Preconditions.checkArgument(signature != null && ! signature.trim().isEmpty());
            Preconditions.checkArgument(seqNo >= 0);

            this.partitionId = partitionId;
            this.signature = signature;
            this.seqNo = seqNo;
            this.generationId = generationId;
        }

        int getPartitionId() {
            return partitionId;
        }

        String getSignature() {
            return signature;
        }

        long getSequenceNumber() {
            return seqNo;
        }

        long getGenerationId() {
            return generationId;
        }

        VoltMessage asVoltMessage() {
            byte [] signatureBytes = signature.getBytes(Constants.UTF8ENCODING);
            final int msgLen = 1 + 4 + 4 + signatureBytes.length + 8 + 8;
            ByteBuffer buf = ByteBuffer.allocate(msgLen);
            buf.put((byte)ExportManager.RELEASE_BUFFER);
            buf.putInt(partitionId);
            buf.putInt(signatureBytes.length);
            buf.put(signatureBytes);
            buf.putLong(seqNo);
            buf.putLong(generationId);

            return new BinaryPayloadMessage(new byte[0], buf.array());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + partitionId;
            result = prime * result
                    + ((signature == null) ? 0 : signature.hashCode());
            result = prime * result + (int) (seqNo ^ (seqNo >>> 32));
            result = prime * result + (int) (generationId ^ (generationId >>> 32));
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
            if (seqNo != other.seqNo)
                return false;
            if (generationId != other.generationId)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "AckPayloadMessage [partitionId=" + partitionId
                    + ", signature=" + signature + ", seqNo=" + seqNo
                    + ", generationId=" + generationId + "]";
        }
    }
}
