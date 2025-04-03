/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hms.mirror.domain;

import com.cloudera.utils.hms.mirror.MessageCode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;

import java.text.MessageFormat;
import java.util.*;

@Getter
@Setter
public class Messages implements Cloneable {

//    @JsonIgnore
    private BitSet bitSet;
//    @JsonIgnore
    private Map<Integer, Object[]> argMap = new TreeMap<Integer, Object[]>();

    public Messages() {
        bitSet = new BitSet(150);
    }

    public Messages(int size) {
        bitSet = new BitSet(size);
    }

    public void clear() {
        bitSet.clear();
        argMap.clear();
    }

    @JsonIgnore
    public String getMessage(int bit) {
        String rtn = null;
        for (MessageCode messageCode : MessageCode.values()) {
            if (messageCode.ordinal() == bit) {
                if (argMap.containsKey(bit)) {
                    Object[] args = argMap.get(messageCode.ordinal());
                    rtn = MessageFormat.format(messageCode.getDesc(), args);
                } else {
                    rtn = messageCode.getDesc();
                }

            }
        }
        return rtn;
    }

    @JsonIgnore
    public List<String> getMessages() {
        List<String> messageList = new ArrayList<String>();
        for (MessageCode messageCode : MessageCode.getCodes(bitSet)) {
            if (!argMap.containsKey(messageCode.ordinal())) {
                messageList.add(messageCode + "-->" + messageCode.getDesc());
            } else {
                Object[] vMap = argMap.get(messageCode.ordinal());
                String m = MessageFormat.format(messageCode.getDesc(), vMap);
                messageList.add(messageCode + "-->" + m);
            }
        }
//        String[] rtn = messageList.toArray(new String[0]);

        return messageList;
    }

    @JsonIgnore
    public long getReturnCode() {
        long rtn = 0;
        long[] messageSet = bitSet.toLongArray();
        for (long messageBit : messageSet) {
            rtn = rtn | messageBit;
        }
        return rtn * -1;
    }

    public void set(MessageCode messageCode) {
        bitSet.set(messageCode.ordinal());
    }

    public void set(MessageCode messageCode, Object... args) {
        bitSet.set(messageCode.ordinal());
        if (args != null) {
            argMap.put(messageCode.ordinal(), args);
        }
    }

    @Override
    public Messages clone() throws CloneNotSupportedException {
        Messages clone = (Messages) super.clone();
        clone.bitSet = (BitSet) bitSet.clone();
        clone.argMap = new TreeMap<Integer, Object[]>(argMap);
        return clone;
    }
}
