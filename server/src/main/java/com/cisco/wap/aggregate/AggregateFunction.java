package com.cisco.wap.aggregate;

import com.cisco.wap.StoreRequest;
import com.cisco.wap.cache.BigQueueManger;
import com.cisco.wap.cache.BigQueueWrapper;
import com.cisco.wap.cache.DeferredMessage;
import com.cisco.wap.cache.MapDBManger;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public abstract class AggregateFunction extends TimerTask {
    private static final Logger logger = LoggerFactory.getLogger(AggregateFunction.class);
    private final int BATCH_SIZE = 1_000;
    private final int BUFFERING_DURATION = 5_000_000;
    protected String dir;
    protected String tableName;
    protected HTreeMap map;
    protected Stopwatch stopwatch;

    public AggregateFunction(String dir, String tableName) {
        this.tableName = tableName;
        this.dir = dir;
        map = MapDBManger.getInstance(tableName);
        stopwatch = Stopwatch.createUnstarted();
    }

    @Override
    public void run() {
        List<DeferredMessage> requests = Lists.newArrayList();
        BigQueueWrapper queue = BigQueueManger.getInstance(dir, tableName);
        long startTime = System.nanoTime();
        while (requests.size() <= BATCH_SIZE) {
            if(queue.isEmpty()) {
                break;
            }
            DeferredMessage message = queue.get(DeferredMessage.class);
            if(Objects.nonNull(message)) {
                requests.add(message);
            }
            long estimatedTime = System.nanoTime() - startTime;
            if(estimatedTime > BUFFERING_DURATION) {
                break;
            }
        }
        if(requests.isEmpty()) {
            logger.debug("no data, skip");
            return;
        }
        stopwatch.start();
        aggregate(requests);
        stopwatch.stop();
        logger.info("The {} function with {} records spends {} ms in this batch.",
                this.name(),
                requests.size(),
                stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    public void stop() {
        map.close();
        super.cancel();
    }

    protected abstract void aggregate(List<DeferredMessage> requests);
    protected abstract String name();
}
