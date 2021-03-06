/*
*  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*/

package org.ballerinalang.util.debugger.dto;

import org.ballerinalang.bre.nonblocking.debugger.FrameInfo;
import org.ballerinalang.model.NodeLocation;

import java.util.ArrayList;
import java.util.List;

/**
 * DTO class representing the messages sent to client from the debugger.
 *
 * @since 0.8.0
 */
public class MessageDTO {

    private String code;

    private String message;

    private String threadId;

    private BreakPointDTO location;

    private List<FrameDTO> frames = new ArrayList<FrameDTO>();

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getThreadId() {
        return threadId;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }

    public BreakPointDTO getLocation() {
        return location;
    }

    public void setLocation(NodeLocation location) {
        this.location = new BreakPointDTO(location.getPackageDirPath(), location.getFileName(),
                location.getLineNumber());
    }

    public void setLocation(BreakPointDTO location) {
        this.location = location;
    }

    public void setFrames(FrameInfo[] frameInfos) {
        for (FrameInfo frame: frameInfos) {
            frames.add(new FrameDTO(frame));
        }
    }

    public List<FrameDTO> getFrames() {
        return frames;
    }
}
