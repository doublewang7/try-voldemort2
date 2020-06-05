package com.cisco.wap.client;

import com.cisco.wap.StoreRequest;
import com.cisco.wap.StoreResponse;
import com.cisco.wap.VoldemortServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

public class RoutingClient {
    public static void route(ManagedChannel channel, Collection<StoreRequest> requests) throws InterruptedException {
        CountDownLatch finishLatch = new CountDownLatch(1);
        VoldemortServiceGrpc.VoldemortServiceStub stub = VoldemortServiceGrpc.newStub(channel);
        StreamObserver<StoreRequest> router = stub.put(new RoutingStreamObserver(finishLatch));
        requests.stream().forEach(router::onNext);
        router.onCompleted();
        finishLatch.await();
    }

    public static StoreResponse get(ManagedChannel channel, StoreRequest request) {
        VoldemortServiceGrpc.VoldemortServiceBlockingStub stub = VoldemortServiceGrpc.newBlockingStub(channel);
        return stub.get(request);
    }
}
