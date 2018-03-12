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

package org.ballerinalang.test.nativeimpl.functions.io.data;

import org.ballerinalang.nativeimpl.io.channels.base.Channel;
import org.ballerinalang.nativeimpl.io.channels.base.DataChannel;
import org.ballerinalang.test.nativeimpl.functions.io.MockByteChannel;
import org.ballerinalang.test.nativeimpl.functions.io.util.TestUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.ByteChannel;

public class DataInputOutputTest {
    /**
     * Specifies the default directory path.
     */
    private String currentDirectoryPath = "/tmp/";

    @BeforeSuite
    public void setup() {
        currentDirectoryPath = System.getProperty("user.dir") + "/target/";
    }

    @Test(description = "Test reading and writing using data i/o")
    public void testReadDataInput() throws IOException, URISyntaxException {
        //Number of characters in this file would be 6
        ByteChannel byteChannel = TestUtil.openForWriting(currentDirectoryPath + "protocolA.txt");
        Channel channel = new MockByteChannel(byteChannel);
        DataChannel inputChannel = new DataChannel(channel);

        final boolean expectedBoolean = true;
        final Integer expectedInteger = 12;
        final float expectedFloat = 364;

        inputChannel.writeBoolean(expectedBoolean);
        inputChannel.writeInteger(expectedInteger);
        inputChannel.writeFloat(expectedFloat);

        channel.close();

        byteChannel = TestUtil.openFileFromPath(currentDirectoryPath + "protocolA.txt");
        channel = new MockByteChannel(byteChannel);
        inputChannel = new DataChannel(channel);

        boolean actualBoolean = inputChannel.readBoolean();
        Integer actualInteger = inputChannel.readInteger();
        float actualFloat = inputChannel.readFloat();

        channel.close();

        Assert.assertEquals(actualBoolean, expectedBoolean);
        Assert.assertEquals(actualInteger, expectedInteger);
        Assert.assertEquals(actualFloat, expectedFloat);
    }
}
