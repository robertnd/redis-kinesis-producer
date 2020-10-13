package com.safaricom.analytics.streaming.producer.kinesis.impl;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.safaricom.analytics.streaming.producer.kinesis.ProducerService;
import lombok.Data;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Data
public class ProducerServiceImpl implements ProducerService, InitializingBean {
    @Value(value = "${aws.stream-name}")
    private String streamName;

    @Value(value = "${aws.region}")
    private String awsRegion;

    /* Dynamically supplied by the environment */
    @Value(value = "${aws.access-key}")
    private String awsAccessKey;

    /* Dynamically supplied by the environment */
    @Value(value = "${aws.secret-key}")
    private String awsSecretKey;

    @Value(value = "${aws.max-connections}")
    private int maxConnections;

    @Value(value = "${aws.request-timeout-ms}")
    private int requestTimeoutMs;

    @Value(value = "${aws.record-max-buffered-time}")
    private int recordMaxBufferedTime;

    private KinesisProducer producer = null;

    final AtomicLong completed = new AtomicLong(0);
    private static final String PARTITION_KEY = Long.toString(System.currentTimeMillis());

    /*
    public ProducerServiceImpl() {

    }
    */

    private KinesisProducer init() {
        if (producer == null) {

            BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);

            KinesisProducerConfiguration config = new KinesisProducerConfiguration();
            config.setRegion(awsRegion);
            config.setCredentialsProvider(new AWSStaticCredentialsProvider(awsCreds));
            config.setMaxConnections(maxConnections); //1
            config.setRequestTimeout(requestTimeoutMs); // 6 seconds {6000}
            config.setRecordMaxBufferedTime(recordMaxBufferedTime); // 5 seconds {5000}

            producer = new KinesisProducer(config);
        }

        return producer;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.producer = init();
    }

    @Override
    public void kinesisWrite(String payload) throws Exception {

        FutureCallback<UserRecordResult> handler = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                int attempts = ((UserRecordFailedException) t).getResult().getAttempts().size() - 1;
                if (t instanceof UserRecordFailedException) {
                    Attempt last =
                            ((UserRecordFailedException) t).getResult().getAttempts().get(attempts);
                    if (attempts > 1) {
                        Attempt previous = ((UserRecordFailedException) t).getResult().getAttempts()
                                .get(attempts - 1);
                        System.out.println(String.format(
                                "Failed to write record - %s : %s. Previous failure - %s : %s",
                                last.getErrorCode(), last.getErrorMessage(), previous.getErrorCode(), previous.getErrorMessage()));
                    } else {
                        System.out.println(String.format("Failed to write record - %s : %s.", last.getErrorCode(), last.getErrorMessage()));
                    }

                }
            }

            @Override
            public void onSuccess(UserRecordResult result) {

                long totalTime = result.getAttempts().stream()
                        .mapToLong(a -> a.getDelay() + a.getDuration()).sum();
                System.out.println(String.format("Data writing success. Total time taken to write data = %d", totalTime));
                completed.getAndIncrement();
            }
        };

        final ExecutorService handlerPool = Executors.newCachedThreadPool();

        ByteBuffer data = null;

        try {
            data = ByteBuffer.wrap(payload.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        /* while (producer.getOutstandingRecordsCount() > 1e4) {
            Thread.sleep(1);
        }
        */

        ListenableFuture<UserRecordResult> f = producer.addUserRecord(streamName, PARTITION_KEY, data);
        Futures.addCallback(f, handler, handlerPool);
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.flushSync();
            producer.destroy();
        }
    }
}
