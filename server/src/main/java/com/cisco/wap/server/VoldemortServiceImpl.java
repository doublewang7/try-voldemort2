package com.cisco.wap.server;

import com.cisco.wap.StoreRequest;
import com.cisco.wap.StoreResponse;
import com.cisco.wap.Type;
import com.cisco.wap.VoldemortServiceGrpc;
import com.cisco.wap.cache.MapDBManger;
import com.cisco.wap.client.RoutingClient;
import com.cisco.wap.config.VoldemortConfig;
import com.cisco.wap.route.ConsistentHashRouter;
import com.cisco.wap.route.VoldemortNode;
import com.google.common.collect.Maps;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.eclipse.collections.api.map.MutableMap;
import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class VoldemortServiceImpl extends VoldemortServiceGrpc.VoldemortServiceImplBase {
    private static Logger logger = LoggerFactory.getLogger(VoldemortServiceImpl.class);
    private VoldemortNode self;
    private ConsistentHashRouter<VoldemortNode> router;
    private VoldemortConfig config;
    private Map<VoldemortNode, ManagedChannel> channels;

    public VoldemortServiceImpl(VoldemortConfig config,
                                VoldemortNode self,
                                ConsistentHashRouter<VoldemortNode> router,
                                Map<VoldemortNode, ManagedChannel> channels) {
        this.config = config;
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

    public static VoldemortServiceImplBuilder builder() {
        return new VoldemortServiceImplBuilder();
    }

    public static class VoldemortServiceImplBuilder {
        private ConsistentHashRouter<VoldemortNode> router;
        private VoldemortNode self;
        private VoldemortConfig config;
        private Map<VoldemortNode, ManagedChannel> channels;

        VoldemortServiceImplBuilder(){}

        public VoldemortServiceImplBuilder node(VoldemortNode node) {
            this.self = node;
            if(Objects.isNull(router)) {
                this.router = new ConsistentHashRouter<>(Collections.singleton(this.self));
            }
            return this;
        }

        public VoldemortServiceImplBuilder config(VoldemortConfig config) {
            this.config = config;
            return this;
        }

        public VoldemortServiceImplBuilder router(ConsistentHashRouter<VoldemortNode> router) {
            if(Objects.nonNull(router)) {
                this.router = router;
            }
            return this;
        }

        public VoldemortServiceImplBuilder channels(Map<VoldemortNode, ManagedChannel> channels) {
            this.channels = Maps.newConcurrentMap();
            this.channels.putAll(channels);
            return this;
        }

        public VoldemortServiceImpl build() {
            return new VoldemortServiceImpl(this.config, this.self, this.router, this.channels);
        }
    }
}
