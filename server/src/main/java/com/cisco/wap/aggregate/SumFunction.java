package com.cisco.wap.aggregate;

import com.cisco.wap.StoreRequest;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class SumFunction extends AggregateFunction {

    public SumFunction(String cubeName, List<StoreRequest> requests) {
        super(cubeName, requests);
    }

    @Override
    protected void aggregate() {
        Map<String, Long> aggregated = this.requests.stream()
                .collect(groupingBy(StoreRequest::getPayload, counting()));
        aggregated.entrySet().forEach(i -> {
            String key = i.getKey();
            Long value = i.getValue();
            Long original = (Objects.nonNull(map.get(key))) ? Long.parseLong(map.get(key).toString()) : 0L ;
            map.put(i.getKey(), value + original);
        });
    }
}
