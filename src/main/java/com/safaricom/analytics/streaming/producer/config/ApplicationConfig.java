package com.safaricom.analytics.streaming.producer.config;

import lombok.Data;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

//import javax.annotation.PostConstruct;
//import java.net.InetAddress;
//import java.util.UUID;
//import java.net.UnknownHostException;


@Configuration
@EnableAutoConfiguration
@ConfigurationProperties(prefix = "")
public @Data class ApplicationConfig {

    private String streamName;
    private String consumerGroupName;
    private String redisHost;
    private String redisPassword;
    private int redisPort;
    private String secretKey;
    private String dataKey;
    private String dataType;
    private String recordCacheKey;
    private long streamPollTimeout;
    private String consumerName;
    private String authRequired;

    /*
    @PostConstruct
    public void setConsumerName() throws UnknownHostException {
        consumerName = InetAddress.getLocalHost().getHostName() + UUID.randomUUID();
    }*/
}

