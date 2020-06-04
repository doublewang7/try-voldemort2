package com.cisco.wap.server;

import com.cisco.wap.StoreRequest;
import com.cisco.wap.StoreResponse;
import com.cisco.wap.Type;
import com.cisco.wap.cache.MapDBManger;
import com.cisco.wap.client.RoutingClient;
import com.cisco.wap.client.RoutingStreamObserver;
import com.cisco.wap.route.ConsistentHashRouter;
import com.cisco.wap.route.VoldemortNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class PutStreamObserver implements StreamObserver<StoreRequest> {
    private static Logger logger = LoggerFactory.getLogger(PutStreamObserver.class);
    private Multimap<ManagedChannel, StoreRequest> requests;
    private ConsistentHashRouter<VoldemortNode> router;
    private VoldemortNode selfNode;
    private Map<VoldemortNode, ManagedChannel> channels;
    private StreamObserver<StoreResponse> responseObserver;

    public PutStreamObserver(ConsistentHashRouter<VoldemortNode> router,
                             VoldemortNode selfNode,
                             Map<VoldemortNode, ManagedChannel> channels,
                             StreamObserver<StoreResponse> responseObserver) {
        requests = ArrayListMultimap.create();
        this.selfNode = selfNode;
        this.channels = channels;
        this.router = router;
        this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(StoreRequest request) {
        ManagedChannel channel = channels.get(selfNode);
        // if the message is routed message, skip look up the target node
        if(request.getType() == Type.DIRECT) {
            VoldemortNode voldemortNode = router.routeRequest(request.getKey());
            channel = channels.get(voldemortNode);
        }
        requests.put(channel, request);
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onCompleted() {
        ManagedChannel selfChannel = channels.get(selfNode);
        requests.asMap().entrySet().stream().filter(i -> !selfChannel.equals(i.getKey())).forEach(i -> {
            try {
                RoutingClient.route(i.getKey(), i.getValue());
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        });
        Collection<StoreRequest> storeRequests = requests.get(selfChannel);
        Map<String, List<StoreRequest>> tables = storeRequests.stream().collect(groupingBy(StoreRequest::getTable));
        tables.entrySet().stream().forEach(table -> {
            HTreeMap map = MapDBManger.getInstance(table.getKey());
            Map<String, Long> aggregated = table.getValue().stream().collect(groupingBy(StoreRequest::getPayload, counting()));
            aggregated.entrySet().forEach(i -> {
                String key = i.getKey();
                Long value = i.getValue();
                Long original = (Objects.nonNull(map.get(key))) ? Long.parseLong(map.get(key).toString()) : 0L ;
                map.put(i.getKey(), value + original);
            });
        });
        int recordNum = requests.get(selfChannel).size();
        StoreResponse response = StoreResponse.newBuilder()
                .setNodeId(selfNode.getId())
                .setPayload(String.format("%s wrote %d records.", selfNode, recordNum))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
