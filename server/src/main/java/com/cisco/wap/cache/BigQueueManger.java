package com.cisco.wap.cache;

import com.cisco.wap.aggregate.SumFunction;
import com.google.common.collect.Maps;
import com.leansoft.bigqueue.BigQueueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

public class BigQueueManger {
    private static final Logger logger = LoggerFactory.getLogger(BigQueueManger.class);
    public static final long NO_DELAY = 0L;
    public static final long ONE_MINUTE = 10_000L;
    private static Map<String, BigQueueWrapper> queues = Maps.newConcurrentMap();
    private static Timer timer = new Timer();

    public static BigQueueWrapper getInstance(String dir, String name) {
        synchronized (BigQueueImpl.class) {
            BigQueueWrapper bigQueue = queues.get(name);
            if(Objects.nonNull(bigQueue)) {
                return bigQueue;
            }
            BigQueueWrapper wrapper = null;
            try {
                wrapper = new BigQueueWrapper(dir, name);
                queues.put(name, wrapper);
                TimerTask timerTask = new SumFunction(dir, name);
                timer.schedule(timerTask, NO_DELAY, ONE_MINUTE);
                logger.info("the local folder {} is created for defer queue.", dir);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
            return wrapper;
        }
    }
}
