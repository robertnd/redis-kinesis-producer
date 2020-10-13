package com.safaricom.analytics.streaming.producer.model;


import lombok.Data;

import java.io.Serializable;

@Data
public class Message implements Serializable {
    private String transactionType;
    private boolean callback;
    private String sourceSystem;
    private String serviceName;
    private String msisdn;
    private String transactionId;
    private String productId;
    private String amount;
    private String channel;
    private String checkoutRequestID;
    private String merchantRequestID;
}
