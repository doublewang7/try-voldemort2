package com.cisco.wap.cache;

import com.cisco.wap.config.VoldemortConfig;
import com.google.common.collect.Maps;
import com.leansoft.bigqueue.BigQueueImpl;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class BigQueueManger {
    private static Map<String, BigQueueWrapper> queues = Maps.newConcurrentMap();

    public static BigQueueWrapper getInstance(String dir, String name) throws IOException {
        synchronized (BigQueueImpl.class) {
            BigQueueWrapper bigQueue = queues.get(name);
            if(Objects.nonNull(bigQueue)) {
                return bigQueue;
            }
            BigQueueWrapper wrapper = new BigQueueWrapper(dir, name);
            queues.put(name, wrapper);
            return wrapper;
        }
    }
}
