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
import org.apache.calcite.linq4j.Linq4j;
import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class VoldemortServiceImpl extends VoldemortServiceGrpc.VoldemortServiceImplBase {
    private static Logger logger = LoggerFactory.getLogger(VoldemortServiceImpl.class);
    private ConsistentHashRouter<VoldemortNode> router;
    private Map<VoldemortNode, ManagedChannel> channels;
    private VoldemortNode self;
    private VoldemortMediator mediator;


    public VoldemortServiceImpl(VoldemortMediator mediator, VoldemortNode self) {
        this.mediator = mediator;
        this.self = self;
        this.router = mediator.getRouter();
        this.channels = mediator.getChannels();
    }

    @Override
    public void get(StoreRequest request, StreamObserver<StoreResponse> responseObserver) {
        AtomicLong value = new AtomicLong(0);
        ManagedChannel selfChannel = channels.get(self);
        if(request.getType() == Type.DIRECT) {
            List<ManagedChannel> others = Linq4j.asEnumerable(channels.values())
                    .where(x -> !selfChannel.equals(x))
                    .toList();
            others.parallelStream().forEach(channel -> {
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
        return new PutStreamObserver(this.mediator, this.self, responseObserver);
    }

    public static VoldemortServiceImplBuilder builder() {
        return new VoldemortServiceImplBuilder();
    }

    public static class VoldemortServiceImplBuilder {
        private VoldemortNode self;
        private VoldemortMediator mediator;

        VoldemortServiceImplBuilder(){}

        public VoldemortServiceImplBuilder node(VoldemortNode node) {
            this.self = node;
            return this;
        }

        public VoldemortServiceImplBuilder mediator(VoldemortMediator mediator) {
            this.mediator = mediator;
            return this;
        }

        public VoldemortServiceImpl build() {
            return new VoldemortServiceImpl(this.mediator, this.self);
        }
    }
}
