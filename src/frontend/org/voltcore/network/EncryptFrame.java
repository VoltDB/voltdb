/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltcore.network;

import java.util.List;

import com.google_voltpatches.common.collect.ImmutableList;

import io.netty_voltpatches.buffer.ByteBuf;

/**
 * Container and tracking class used during message encryptions
 */
class EncryptFrame {

    final static int UNCHUNKED_SIGIL = -1;
    /**
     * chunk number in a sequence of chunks. If a message is split into 3 chunks
     * then their respective chunk numbers would be 1, 2, and 3. Each chunk
     * chunks value would be 3
     */
    final int chunkno;
    /**
     * chunk number in a sequence of chunks. If a message is split into 3 chunks
     * then their respective chunk numbers would be 1, 2, and 3. Each chunk
     * chunks value would be 3
     */
    final int chunks;
    /**
     * Once encrypted this value signifies the difference in bytes between
     * the encrypted chunk, and the non encrypted source
     */
    final int delta;
    /**
     * SSLEngine encrypt can only encrypt up to 16k at a time, so a frame
     * contains an encrypt operation source buffer which is a slice window
     * into the underlying bb buffer
     */
    final ByteBuf frame;
    final ByteBuf bb;
    /**
     * Indicates how many Volt messages this frame contains
     */
    final int msgs;


    EncryptFrame(ByteBuf source, int msgs) {
        this(1, 1, UNCHUNKED_SIGIL, msgs, source, source);
    }

    EncryptFrame(ByteBuf source, int delta, int msgs) {
        this(1, 1, delta, msgs, source, source);
    }

    public boolean isChunked() {
        return delta != UNCHUNKED_SIGIL;
    }

    public boolean inEncrypted() {
        return bb == null;
    }

    public boolean isLast() {
        return isChunked() && chunkno == chunks;
    }

    List<EncryptFrame> chunked(final int frameMax) {
        if (!bb.isReadable()) {
            return ImmutableList.of();
        }
        if (bb.readableBytes() <= frameMax) {
            return ImmutableList.of(new EncryptFrame(1, 1, 0, msgs, bb, bb));
        }
        int frames = bb.writerIndex() / frameMax;
        frames = bb.writerIndex() % frameMax == 0 ? frames : frames+1;
        ImmutableList.Builder<EncryptFrame> lbld = ImmutableList.builder();
        for (int chunk = 1; chunk <= frames; ++chunk) {
            int sliceSz = Math.min(frameMax, bb.readableBytes());
            // slices are views into the underlying byte buf (bb)
            EncryptFrame piece = new EncryptFrame(chunk, frames, 0, msgs, bb.readSlice(sliceSz), bb);
            lbld.add(piece);
        }
        return lbld.build();
    }

    EncryptFrame encrypted(int delta, ByteBuf encrypted) {
        return new EncryptFrame(chunkno, chunks, delta, msgs, encrypted, null);
    }

    private EncryptFrame(int chunkno, int chunks, int delta, int msgs, ByteBuf frame, ByteBuf sliceSource) {
        this.chunkno = chunkno;
        this.chunks = chunks;
        this.delta = delta;
        this.frame = frame;
        this.bb = sliceSource;
        this.msgs = msgs;
    }

    @Override
    public String toString() {
        return "EncryptFrame [chunkno=" + chunkno + ", chunks=" + chunks
                + ", msgs=" + msgs + ", delta=" + delta + ", slice=" + frame + "]";
    }
}