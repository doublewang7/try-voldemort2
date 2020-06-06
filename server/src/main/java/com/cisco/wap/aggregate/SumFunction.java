package com.cisco.wap.aggregate;

import com.cisco.wap.StoreRequest;
import com.cisco.wap.cache.DeferredMessage;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class SumFunction extends AggregateFunction {


    public SumFunction(String dir, String tableName) {
        super(dir, tableName);
    }

    @Override
    protected void aggregate(List<DeferredMessage> requests) {
        Map<String, Long> aggregated = requests.stream()
                .collect(groupingBy(DeferredMessage::getValue, counting()));
        aggregated.entrySet().forEach(i -> {
            String key = i.getKey();
            Long value = i.getValue();
            Long original = (Objects.nonNull(map.get(key))) ? Long.parseLong(map.get(key).toString()) : 0L ;
            map.put(i.getKey(), value + original);
        });
    }

    @Override
    protected String name() {
        return "sum";
    }
}
