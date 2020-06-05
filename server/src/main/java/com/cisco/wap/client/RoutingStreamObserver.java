package com.cisco.wap.client;

import com.cisco.wap.StoreResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class RoutingStreamObserver implements StreamObserver<StoreResponse>  {
    private static Logger logger = LoggerFactory.getLogger(RoutingStreamObserver.class);
    private final CountDownLatch latch;

    public RoutingStreamObserver(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void onNext(StoreResponse storeResponse) {
        logger.debug(storeResponse.getPayload());
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error(throwable.getMessage(), throwable);
    }

    @Override
    public void onCompleted() {
        this.latch.countDown();
    }
}
