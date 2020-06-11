package com.cisco.wap;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class SimpleClient {
    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6666)
                .usePlaintext()
                .build();

        VoldemortServiceGrpc.VoldemortServiceBlockingStub stub
                = VoldemortServiceGrpc.newBlockingStub(channel);

        StoreRequest request = StoreRequest.newBuilder()
                .setType(Type.DIRECT)
                .setTable("FirstRun")
                .setKey("cat")
                .build();

        StoreResponse storeResponse = stub.get(request);
        System.out.println(storeResponse.getPayload()+"@"+storeResponse.getNodeId());
    }
}
