/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.nativeimpl.io.channels.base;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Represents a channel which will serialize the data which to the specified type.
 */
public class DataChannel {
    /**
     * Channel which will be used to read/write bytes.
     */
    private Channel channel;

    public DataChannel(Channel channel) {
        this.channel = channel;
    }

    /**
     * Will read bytes to the buffer until the buffer is filled or the channel reach it's end.
     *
     * @param buffer the buffer which will read bytes.
     * @throws IOException during I/O error
     */
    private void readFull(ByteBuffer buffer) throws IOException {
        do {
            channel.read(buffer);
        } while (buffer.hasRemaining());
        buffer.flip();
    }

    /**
     * Writes the given bytes to the channel.
     *
     * @param buffer the buffer which holds the content which should be written.
     * @throws IOException during error
     */
    private void writeFull(ByteBuffer buffer) throws IOException {
        buffer.flip();
        do {
            channel.write(buffer);
        } while (buffer.hasRemaining());
    }

    /**
     * Reads 1 bytes and return it's boolean value.
     *
     * @return true if the byte value is 1, false otherwise.
     * @throws IOException during I/O error.
     */
    public boolean readBoolean() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        readFull(buffer);
        byte value = buffer.get();
        return value == 1;
    }

    /**
     * Writes boolean value to the file.
     *
     * @param value value of the boolean.
     */
    public void writeBoolean(boolean value) throws IOException {
        byte booleanValue = (value) ? (byte) 1 : 0;
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(booleanValue);
        writeFull(buffer);
    }

    /**
     * Reads 4 input bytes and return the float value.
     *
     * @return the value of the float.
     * @throws IOException during I/O error.
     */
    public float readFloat() throws IOException {
        ByteBuffer content = ByteBuffer.allocate(4);
        readFull(content);
        return content.getFloat();
    }

    /**
     * Writes the float value to the provided channel.
     *
     * @param value the float value which should be written to the channel.
     * @throws IOException during I/O error.
     */
    public void writeFloat(float value) throws IOException {
        ByteBuffer content = ByteBuffer.allocate(4);
        content.putFloat(value);
        writeFull(content);
    }

    /**
     * Reads 4 input bytes and return it's integer value.
     *
     * @return the value of the integer.
     * @throws IOException during I/O error.
     */
    public Integer readInteger() throws IOException {
        ByteBuffer content = ByteBuffer.allocate(4);
        readFull(content);
        return content.getInt();
    }

    /**
     * Writes integer for the provided source.
     *
     * @param value the value of the integer which should be written.
     */
    public void  writeInteger(int value) throws IOException {
        ByteBuffer content = ByteBuffer.allocate(4);
        content.putInt(value);
        writeFull(content);
    }

    /**
     * Reads a byte and return it's value.
     *
     * @return the byte which is read.
     * @throws IOException during I/O error.
     */
    public byte readByte() throws IOException {
        ByteBuffer content = ByteBuffer.allocate(1);
        readFull(content);
        return content.get();
    }

    public void writeByte(byte value) throws IOException {
        ByteBuffer content = ByteBuffer.allocate(1);
        content.put(value);
        writeFull(content);
    }

}
