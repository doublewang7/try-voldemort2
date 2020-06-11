package com.cisco.wap.aggregate;

import com.cisco.wap.cache.DeferredMessage;
import com.cisco.wap.utils.tuple.Tuple2;
import org.apache.calcite.linq4j.Linq4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SumFunction extends AggregateFunction {

    public SumFunction(String dir, String tableName) {
        super(dir, tableName);
    }

    @Override
    protected void aggregate(List<DeferredMessage> requests) {
        List<String> values = requests.stream().map(DeferredMessage::getValue)
                .collect(Collectors.toList());
        List<Tuple2> result = Linq4j.asEnumerable(values)
                .groupBy(
                        x -> x,
                        () -> 0,
                        (v, e) -> v + 1,
                        (k, v) -> {
                            Long original = (Objects.nonNull(map.get(k))) ? Long.parseLong(map.get(k).toString()) : 0L;
                            return new Tuple2(k, original + v);
                        }
                ).toList();
        Map<Object, Object> merged = result.stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
        map.putAll(merged);
    }

    @Override
    protected String name() {
        return "sum";
    }
}
