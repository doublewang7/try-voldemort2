package com.cisco.wap.aggregate;

import com.cisco.wap.StoreRequest;
import com.cisco.wap.cache.MapDBManger;
import com.google.common.collect.Lists;
import org.mapdb.HTreeMap;

import java.util.List;
import java.util.TimerTask;

public abstract class AggregateFunction extends TimerTask {
    protected String cubeName;
    protected List<StoreRequest> requests;
    protected HTreeMap map;

    public AggregateFunction(String cubeName, List<StoreRequest> requests) {
        this.cubeName = cubeName;
        this.requests = Lists.newArrayList();
        this.requests.addAll(requests);
        map = MapDBManger.getInstance(cubeName);
    }

    @Override
    public void run() {
        aggregate();
    }

    public void stop() {
        map.close();
        super.cancel();
    }

    protected abstract void aggregate();
}
