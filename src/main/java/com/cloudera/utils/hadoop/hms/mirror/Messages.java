package com.cloudera.utils.hadoop.hms.mirror;

import java.text.MessageFormat;
import java.util.*;

public class Messages {

    private BitSet bitSet;
    private Map<Integer, Object[]> argMap = new TreeMap<Integer, Object[]>();

    public Messages(int size) {
        bitSet = new BitSet(size);
    }

    public void set(int bit, Object... args) {
        bitSet.set(bit);
        if (args != null) {
            argMap.put(bit, args);
        }
    }

    public void set(int bit) {
        bitSet.set(bit);
    }

    public long getReturnCode() {
        long rtn = 0;
        long[] messageSet = bitSet.toLongArray();
        for (long messageBit : messageSet) {
            rtn = rtn | messageBit;
        }
        return rtn;
    }

    public String getMessage(int bit) {
        String rtn = null;
        for (MessageCode messageCode : MessageCode.values()) {
            if (messageCode.getCode() == bit) {
                if (argMap.containsKey(bit)) {
                    Object[] args = argMap.get(messageCode.getCode());
                    rtn = MessageFormat.format(messageCode.getDesc(), args);
                } else {
                    rtn = messageCode.getDesc();
                }

            }
        }
        return rtn;
    }

    public String[] getMessages() {
        List<String> messageList = new ArrayList<String>();
        for (MessageCode messageCode : MessageCode.getCodes(bitSet)) {
            if (argMap.containsKey(messageCode.getCode())) {
                messageList.add(messageCode.getDesc());
            } else {
                messageList.add(MessageFormat.format(messageCode.getDesc(), argMap.get(messageCode.getCode())));
            }
        }
        String[] rtn = messageList.toArray(new String[0]);

        return rtn;
    }

}
