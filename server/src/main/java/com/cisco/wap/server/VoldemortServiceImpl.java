package com.cisco.wap.server;

import com.cisco.wap.StoreRequest;
import com.cisco.wap.StoreResponse;
import com.cisco.wap.Type;
import com.cisco.wap.VoldemortServiceGrpc;
import com.cisco.wap.cache.MapDBManger;
import com.cisco.wap.client.RoutingClient;
import com.cisco.wap.route.ConsistentHashRouter;
import com.cisco.wap.route.VoldemortNode;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

public class VoldemortServiceImpl extends VoldemortServiceGrpc.VoldemortServiceImplBase {
    private static Logger logger = LoggerFactory.getLogger(VoldemortServiceImpl.class);
    private ConsistentHashRouter<VoldemortNode> router;
    private VoldemortNode self;
    private Map<VoldemortNode, ManagedChannel> channels;

    public VoldemortServiceImpl(VoldemortNode self,
                                ConsistentHashRouter<VoldemortNode> router,
                                Map<VoldemortNode, ManagedChannel> channels) {
        this.self = self;
        this.router = router;
        this.channels = channels;
    }

    @Override
    public void get(StoreRequest request, StreamObserver<StoreResponse> responseObserver) {
//        StoreResponse response = null;
//        VoldemortNode node = self;
//        // if the message is routed message, skip look up the target node
//        if(request.getType() == Type.DIRECT) {
//            node = router.routeRequest(request.getKey());
//        }
//        if(node!=self) {
//            ManagedChannel remoteChannel = channels.get(node);
//            response = RoutingClient.get(remoteChannel, request);
//        } else {
//            HTreeMap map = MapDBManger.getInstance(request.getTable());
//            Object value = map.get(request.getKey());
//            response = StoreResponse.newBuilder()
//                    .setNodeId(self.getId())
//                    .setPayload(value.toString())
//                    .build();
//        }
        AtomicLong value = new AtomicLong(0);
        ManagedChannel selfChannel = channels.get(self);
        if(request.getType() == Type.DIRECT) {
            channels.values().parallelStream().filter(i->!selfChannel.equals(i)).forEach(channel -> {
                StoreRequest remoteRequest = StoreRequest.newBuilder()
                        .setType(Type.ROUTED)
                        .setTable(request.getTable())
                        .setKey(request.getKey())
                        .setPayload(request.getPayload())
                        .build();
                StoreResponse response = RoutingClient.get(channel, remoteRequest);
                value.addAndGet(Long.parseLong(response.getPayload()));
            });
        }
        HTreeMap map = MapDBManger.getInstance(request.getTable());
        Object val = map.get(request.getKey());
        if(Objects.nonNull(val)) {
            value.addAndGet(Long.parseLong(val.toString()));
        }
        StoreResponse response = StoreResponse.newBuilder()
                .setNodeId(self.getId())
                .setPayload(""+value.get())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<StoreRequest> put(StreamObserver<StoreResponse> responseObserver) {
        return new PutStreamObserver(this.router, this.self, this.channels, responseObserver);
    }
}
