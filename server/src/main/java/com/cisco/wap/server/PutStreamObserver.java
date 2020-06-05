package com.cisco.wap.server;

import com.cisco.wap.StoreRequest;
import com.cisco.wap.StoreResponse;
import com.cisco.wap.Type;
import com.cisco.wap.aggregate.SumFunction;
import com.cisco.wap.cache.BigQueueManger;
import com.cisco.wap.cache.BigQueueWrapper;
import com.cisco.wap.cache.DeferredMessage;
import com.cisco.wap.client.RoutingClient;
import com.cisco.wap.route.ConsistentHashRouter;
import com.cisco.wap.route.VoldemortNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.util.stream.Collectors.groupingBy;


public class PutStreamObserver implements StreamObserver<StoreRequest> {
    private static Logger logger = LoggerFactory.getLogger(PutStreamObserver.class);
    private Multimap<ManagedChannel, StoreRequest> requests;
    private ConsistentHashRouter<VoldemortNode> router;
    private Map<VoldemortNode, ManagedChannel> channels;
    private StreamObserver<StoreResponse> responseObserver;
    private VoldemortNode selfNode;
    private String dir;

    public PutStreamObserver(VoldemortMediator mediator,
                             VoldemortNode selfNode,
                             StreamObserver<StoreResponse> responseObserver) {
        requests = ArrayListMultimap.create();
        this.selfNode = selfNode;
        this.responseObserver = responseObserver;
        this.router = mediator.getRouter();
        this.channels = mediator.getChannels();
        this.dir = mediator.getConfig().getBigQueueStorePath();
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
            TimerTask timerTask = new SumFunction(table.getKey(), table.getValue());
            BigQueueWrapper queue = BigQueueManger.getInstance(dir, table.getKey(), timerTask);
            if(Objects.isNull(queue)) {
                //TODO: some backup solution when deferred queue not ready
            }
            table.getValue().stream().forEach(request -> {
                DeferredMessage message = DeferredMessage.builder()
                        .key(request.getKey())
                        .value(request.getPayload())
                        .build();
                queue.put(message);
            });
            queue.closeSlience();
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
