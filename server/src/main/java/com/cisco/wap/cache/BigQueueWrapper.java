package com.cisco.wap.cache;

import com.cisco.wap.config.VoldemortConfig;
import com.leansoft.bigqueue.BigQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class BigQueueWrapper implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(BigQueueWrapper.class);
    private BigQueueImpl innerQueue;
    private String path;
    private String name;

    public BigQueueWrapper(String dir, String name) throws IOException {
        this.name = name;
        this.path = dir;
        innerQueue = new BigQueueImpl(this.path, this.name);
    }

    public <T extends Deferrable> void put(T t) {
       try {
            innerQueue.enqueue(t.toByte());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public <T extends Deferrable> T get(Class<T> cls) {
        try {
            byte[] bytes = innerQueue.dequeue();
            T t = cls.newInstance();
            return (T) t.fromByte(bytes);
        } catch (IOException | InstantiationException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        innerQueue.close();
    }
}
