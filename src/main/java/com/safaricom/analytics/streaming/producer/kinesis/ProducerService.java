package com.safaricom.analytics.streaming.producer.kinesis;

public interface ProducerService {
    public void kinesisWrite(String payload) throws Exception;
    public void stop();
}
