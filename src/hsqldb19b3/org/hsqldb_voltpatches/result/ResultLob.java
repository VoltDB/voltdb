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


package org.hsqldb_voltpatches.result;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.lib.DataOutputStream;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;

/**
 * Sub-class of Result for communicating Blob and Clob operations.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class ResultLob extends Result {

    public static interface LobResultTypes {

        int REQUEST_GET_BYTES                 = 1;
        int REQUEST_SET_BYTES                 = 2;
        int REQUEST_GET_CHARS                 = 3;
        int REQUEST_SET_CHARS                 = 4;
        int REQUEST_GET_BYTE_PATTERN_POSITION = 5;
        int REQUEST_GET_CHAR_PATTERN_POSITION = 6;
        int REQUEST_CREATE_BYTES              = 7;
        int REQUEST_CREATE_CHARS              = 8;
        int REQUEST_TRUNCATE                  = 9;
        int REQUEST_GET_LENGTH                = 10;
        int REQUEST_GET_LOB                   = 11;

        //
        int RESPONSE_GET_BYTES                 = 21;
        int RESPONSE_SET                       = 22;
        int RESPONSE_GET_CHARS                 = 23;
        int RESPONSE_GET_BYTE_PATTERN_POSITION = 25;
        int RESPONSE_GET_CHAR_PATTERN_POSITION = 26;
        int RESPONSE_CREATE_BYTES              = 27;
        int RESPONSE_CREATE_CHARS              = 28;
        int RESPONSE_TRUNCATE                  = 29;
    }

    long        lobID;
    int         subType;
    long        blockOffset;
    long        blockLength;
    byte[]      byteBlock;
    char[]      charBlock;
    Reader      reader;
    InputStream stream;

    private ResultLob() {
        mode = ResultConstants.LARGE_OBJECT_OP;
    }

    public static ResultLob newLobGetLengthRequest(long id) {

        ResultLob result = new ResultLob();

        result.subType = LobResultTypes.REQUEST_GET_LENGTH;
        result.lobID   = id;

        return result;
    }

    public static ResultLob newLobGetBytesRequest(long id, long offset,
            int length) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.REQUEST_GET_BYTES;
        result.lobID       = id;
        result.blockOffset = offset;
        result.blockLength = length;

        return result;
    }

    public static ResultLob newLobGetCharsRequest(long id, long offset,
            int length) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.REQUEST_GET_CHARS;
        result.lobID       = id;
        result.blockOffset = offset;
        result.blockLength = length;

        return result;
    }

    public static ResultLob newLobSetBytesRequest(long id, long offset,
            byte block[]) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.REQUEST_SET_BYTES;
        result.lobID       = id;
        result.blockOffset = offset;
        result.byteBlock   = block;
        result.blockLength = block.length;

        return result;
    }

    public static ResultLob newLobSetCharsRequest(long id, long offset,
            char[] chars) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.REQUEST_SET_CHARS;
        result.lobID       = id;
        result.blockOffset = offset;
        result.charBlock   = chars;
        result.blockLength = chars.length;

        return result;
    }

    public static ResultLob newLobTruncateRequest(long id, long offset) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.REQUEST_TRUNCATE;
        result.lobID       = id;
        result.blockOffset = offset;

        return result;
    }

    public static ResultLob newLobGetBytesResponse(long id, long offset,
            byte block[]) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.RESPONSE_GET_BYTES;
        result.lobID       = id;
        result.blockOffset = offset;
        result.byteBlock   = block;
        result.blockLength = block.length;

        return result;
    }

    public static ResultLob newLobGetCharsResponse(long id, long offset,
            char[] chars) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.RESPONSE_GET_CHARS;
        result.lobID       = id;
        result.blockOffset = offset;
        result.charBlock   = chars;
        result.blockLength = chars.length;

        return result;
    }

    public static ResultLob newLobSetResponse(long id, long length) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.RESPONSE_SET;
        result.lobID       = id;
        result.blockLength = length;

        return result;
    }

    public static ResultLob newLobGetBytePatternPositionRequest(long id,
            byte[] pattern, long offset) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.REQUEST_GET_BYTE_PATTERN_POSITION;
        result.lobID       = id;
        result.blockOffset = offset;
        result.byteBlock   = pattern;
        result.blockLength = pattern.length;

        return result;
    }

    public static ResultLob newLobGetCharPatternPositionRequest(long id,
            char[] pattern, long offset) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.REQUEST_GET_CHAR_PATTERN_POSITION;
        result.lobID       = id;
        result.blockOffset = offset;
        result.charBlock   = pattern;
        result.blockLength = pattern.length;

        return result;
    }

    public static ResultLob newLobCreateBlobRequest(long sessionID,
            long lobID, InputStream stream, long length) {

        ResultLob result = new ResultLob();

        result.lobID       = lobID;
        result.subType     = LobResultTypes.REQUEST_CREATE_BYTES;
        result.blockLength = length;
        result.stream      = stream;

        return result;
    }

    public static ResultLob newLobCreateClobRequest(long sessionID,
            long lobID, Reader reader, long length) {

        ResultLob result = new ResultLob();

        result.lobID       = lobID;
        result.subType     = LobResultTypes.REQUEST_CREATE_CHARS;
        result.blockLength = length;
        result.reader      = reader;

        return result;
    }

    public static ResultLob newLobCreateBlobResponse(long id) {

        ResultLob result = new ResultLob();

        result.subType = LobResultTypes.RESPONSE_CREATE_BYTES;
        result.lobID   = id;

        return result;
    }

    public static ResultLob newLobCreateClobResponse(long id) {

        ResultLob result = new ResultLob();

        result.subType = LobResultTypes.RESPONSE_CREATE_CHARS;
        result.lobID   = id;

        return result;
    }

    public static ResultLob newLobTruncateResponse(long id) {

        ResultLob result = new ResultLob();

        result.subType = LobResultTypes.RESPONSE_TRUNCATE;
        result.lobID   = id;

        return result;
    }

    public static ResultLob newLobGetRequest(long id, long offset,
            long length) {

        ResultLob result = new ResultLob();

        result.subType     = LobResultTypes.REQUEST_GET_LOB;
        result.lobID       = id;
        result.blockOffset = offset;
        result.blockLength = length;

        return result;
    }

    public static ResultLob newLob(DataInput dataInput,
                                   boolean readTerminate)
                                   throws IOException {

        ResultLob result = new ResultLob();

        result.databaseID = dataInput.readInt();
        result.sessionID  = dataInput.readLong();
        result.lobID      = dataInput.readLong();
        result.subType    = dataInput.readInt();

        switch (result.subType) {

            case LobResultTypes.REQUEST_CREATE_BYTES :
            case LobResultTypes.REQUEST_CREATE_CHARS :
                result.blockOffset = dataInput.readLong();
                result.blockLength = dataInput.readLong();
                break;

            case LobResultTypes.REQUEST_GET_LOB :

            //
            case LobResultTypes.REQUEST_GET_BYTES :
            case LobResultTypes.REQUEST_GET_CHARS :
                result.blockOffset = dataInput.readLong();
                result.blockLength = dataInput.readLong();
                break;

            case LobResultTypes.REQUEST_SET_BYTES :
            case LobResultTypes.REQUEST_GET_BYTE_PATTERN_POSITION :
                result.blockOffset = dataInput.readLong();
                result.blockLength = dataInput.readLong();
                result.byteBlock   = new byte[(int) result.blockLength];

                dataInput.readFully(result.byteBlock);
                break;

            case LobResultTypes.REQUEST_SET_CHARS :
            case LobResultTypes.REQUEST_GET_CHAR_PATTERN_POSITION :
                result.blockOffset = dataInput.readLong();
                result.blockLength = dataInput.readLong();
                result.charBlock   = new char[(int) result.blockLength];

                for (int i = 0; i < result.charBlock.length; i++) {
                    result.charBlock[i] = dataInput.readChar();
                }
                break;

            case LobResultTypes.REQUEST_GET_LENGTH :
            case LobResultTypes.REQUEST_TRUNCATE :
                result.blockOffset = dataInput.readLong();
                break;

            case LobResultTypes.RESPONSE_GET_BYTES :
                result.blockOffset = dataInput.readLong();
                result.blockLength = dataInput.readLong();
                result.byteBlock   = new byte[(int) result.blockLength];

                dataInput.readFully(result.byteBlock);
                break;

            case LobResultTypes.RESPONSE_GET_CHARS :
                result.blockOffset = dataInput.readLong();
                result.blockLength = dataInput.readLong();
                result.charBlock   = new char[(int) result.blockLength];

                for (int i = 0; i < result.charBlock.length; i++) {
                    result.charBlock[i] = dataInput.readChar();
                }
                break;

            case LobResultTypes.RESPONSE_SET :
            case LobResultTypes.RESPONSE_CREATE_BYTES :
            case LobResultTypes.RESPONSE_CREATE_CHARS :
            case LobResultTypes.RESPONSE_TRUNCATE :
                result.blockLength = dataInput.readLong();
                break;

            case LobResultTypes.RESPONSE_GET_BYTE_PATTERN_POSITION :
            case LobResultTypes.RESPONSE_GET_CHAR_PATTERN_POSITION :
                result.blockOffset = dataInput.readLong();
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ResultLob");
        }

        if (readTerminate) {
            dataInput.readByte();
        }

        return result;
    }

    public void write(DataOutputStream dataOut,
                      RowOutputInterface rowOut)
                      throws IOException {

        writeBody(dataOut);
        dataOut.writeByte(ResultConstants.NONE);
        dataOut.flush();
    }

    public void writeBody(DataOutputStream dataOut)
    throws IOException {

        dataOut.writeByte(mode);
        dataOut.writeInt(databaseID);
        dataOut.writeLong(sessionID);
        dataOut.writeLong(lobID);
        dataOut.writeInt(subType);

        switch (subType) {

            case LobResultTypes.REQUEST_CREATE_BYTES :
                dataOut.writeLong(blockOffset);
                dataOut.writeLong(blockLength);
                dataOut.write(stream, blockLength);
                break;

            case LobResultTypes.REQUEST_CREATE_CHARS :
                dataOut.writeLong(blockOffset);
                dataOut.writeLong(blockLength);
                dataOut.write(reader, blockLength);
                break;

            case LobResultTypes.REQUEST_SET_BYTES :
            case LobResultTypes.REQUEST_GET_BYTE_PATTERN_POSITION :
                dataOut.writeLong(blockOffset);
                dataOut.writeLong(blockLength);
                dataOut.write(byteBlock);
                break;

            case LobResultTypes.REQUEST_SET_CHARS :
            case LobResultTypes.REQUEST_GET_CHAR_PATTERN_POSITION :
                dataOut.writeLong(blockOffset);
                dataOut.writeLong(blockLength);
                dataOut.writeChars(charBlock);
                break;

            case LobResultTypes.REQUEST_GET_LOB :

            //
            case LobResultTypes.REQUEST_GET_BYTES :
            case LobResultTypes.REQUEST_GET_CHARS :
                dataOut.writeLong(blockOffset);
                dataOut.writeLong(blockLength);
                break;

            case LobResultTypes.REQUEST_GET_LENGTH :
            case LobResultTypes.REQUEST_TRUNCATE :
                dataOut.writeLong(blockOffset);
                break;

            case LobResultTypes.RESPONSE_GET_BYTES :
                dataOut.writeLong(blockOffset);
                dataOut.writeLong(blockLength);
                dataOut.write(byteBlock);
                break;

            case LobResultTypes.RESPONSE_GET_CHARS :
                dataOut.writeLong(blockOffset);
                dataOut.writeLong(blockLength);
                dataOut.writeChars(charBlock);
                break;

            case LobResultTypes.RESPONSE_SET :
            case LobResultTypes.RESPONSE_CREATE_BYTES :
            case LobResultTypes.RESPONSE_CREATE_CHARS :
            case LobResultTypes.RESPONSE_TRUNCATE :
                dataOut.writeLong(blockLength);
                break;

            case LobResultTypes.RESPONSE_GET_BYTE_PATTERN_POSITION :
            case LobResultTypes.RESPONSE_GET_CHAR_PATTERN_POSITION :
                dataOut.writeLong(blockOffset);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ResultLob");
        }
    }

    public long getLobID() {
        return lobID;
    }

    public int getSubType() {
        return subType;
    }

    public long getOffset() {
        return blockOffset;
    }

    public long getBlockLength() {
        return blockLength;
    }

    public byte[] getByteArray() {
        return byteBlock;
    }

    public char[] getCharArray() {
        return charBlock;
    }

    public InputStream getInputStream() {
        return stream;
    }

    public Reader getReader() {
        return reader;
    }
}
