package com.cisco.wap;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

public class ClientStreamClient {
    public static void main(String[] args) throws InterruptedException {
        final CountDownLatch finishLatch = new CountDownLatch(1);
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6666)
                .usePlaintext()
                .build();

        VoldemortServiceGrpc.VoldemortServiceStub stub
                = VoldemortServiceGrpc.newStub(channel);

        StreamObserver<StoreRequest> put = stub.put(new StreamObserver<StoreResponse>() {
            @Override
            public void onNext(StoreResponse storeResponse) {
                System.out.println(storeResponse.getPayload());
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
                finishLatch.countDown();
            }
        });

        String[] values = {"cat", "dog", "pig", "cat", "dog", "cat"};
        IntStream.range(0, values.length).forEach(i -> {
            StoreRequest build = StoreRequest.newBuilder()
                    .setType(Type.DIRECT)
                    .setTable("FirstRun")
                    .setKey("voldemort" + i)
                    .setPayload(values[i])
                    .build();
            put.onNext(build);
        });
        put.onCompleted();
        finishLatch.await();
    }
}
