package com.safaricom.analytics.streaming.producer.consumer.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.safaricom.analytics.streaming.producer.config.ApplicationConfig;
import com.safaricom.analytics.streaming.producer.kinesis.ProducerService;
import com.safaricom.analytics.streaming.producer.model.Message;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

@Component
@EnableScheduling
public class RedisStreamConsumer implements StreamListener<String, MapRecord<String, String, String>>, InitializingBean, DisposableBean  {
    @Autowired
    ApplicationConfig config;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private ProducerService producerService;

    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
    private Subscription subscription;
    private String consumerName;
    private String consumerGroupName;
    private String streamName;
    private String dataKey;
    private String dataType;
    final ObjectMapper mapper = new ObjectMapper();


    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        String payload = "";
        try {
            if (dataType.equalsIgnoreCase("keyvalue")) {
                Message messageObj = mapper.convertValue(message.getValue(), Message.class);
                payload = mapper.writeValueAsString(messageObj);
            } else {
                payload = message.getValue().get(dataKey);
            }

            System.out.println("Message: " + payload);
            producerService.kinesisWrite(payload);
            redisTemplate.opsForStream().acknowledge(config.getConsumerGroupName(), message);
            System.out.printf(String.format("Message %s has been processed", message.getId()));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void destroy() throws Exception {
        if (subscription != null) {
            subscription.cancel();
        }

        if (container != null) {
            container.stop();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        consumerName = config.getConsumerName();
        consumerGroupName = config.getConsumerGroupName();
        streamName = config.getStreamName();
        dataKey = config.getDataKey();
        dataType = config.getDataType();

        try {

            if (dataKey.isEmpty()) {
                dataKey = "payload";
            }

            if (!redisTemplate.hasKey(streamName)) {
                System.out.printf("[%s] does not exist. Creating stream along with the consumer group", streamName);
                RedisAsyncCommands commands = (RedisAsyncCommands) redisTemplate.getConnectionFactory()
                        .getConnection().getNativeConnection();
                CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                        .add(CommandKeyword.CREATE)
                        .add(streamName)
                        .add(consumerGroupName)
                        .add("0")
                        .add("MKSTREAM");
                commands.dispatch(CommandType.XGROUP, new StatusOutput<>(StringCodec.UTF8), args);
            } else {
                //creating consumer group
                redisTemplate.opsForStream().createGroup(streamName, ReadOffset.from("0"), consumerGroupName);
            }
        } catch (Exception ex) {
            System.out.printf("[%s] Consumer group already present:", consumerGroupName);
        }

        redisTemplate.setDefaultSerializer(new StringRedisSerializer());

        /*

        if (dataType.equalsIgnoreCase("keyvalue")) {
            RedisMappingContext mappingContext = redisMappingContext();
            RedisConverter redisConverter = redisConverter(mappingContext);
            ObjectHashMapper mapper = hashMapper(redisConverter);

            this.container = streamMapMessageListenerContainer(redisTemplate.getConnectionFactory(), mapper);
        } else {
            this.container = streamJsonMessageListenerContainer();
        }

         */


        this.container = StreamMessageListenerContainer.create(redisTemplate.getConnectionFactory());

        this.subscription = container.receive(
                Consumer.from(consumerGroupName, consumerName),
                StreamOffset.create(streamName, ReadOffset.lastConsumed()),
                this);

        subscription.await(Duration.ofSeconds(2));
        container.start();
    }

    /*
    RedisMappingContext redisMappingContext() {
        RedisMappingContext ctx = new RedisMappingContext();
        ctx.setInitialEntitySet(Collections.singleton(Message.class));
        return ctx;
    }

    RedisConverter redisConverter(RedisMappingContext mappingContext) {
        return new MappingRedisConverter(mappingContext);
    }
    
    ObjectHashMapper hashMapper(RedisConverter converter) {
        return new ObjectHashMapper(converter);
    }
    
    StreamMessageListenerContainer streamMapMessageListenerContainer(RedisConnectionFactory connectionFactory, ObjectHashMapper hashMapper) {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, Object>> options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                .objectMapper(hashMapper)
                .build();

        return StreamMessageListenerContainer.create(connectionFactory, options);
    }
    
    StreamMessageListenerContainer streamJsonMessageListenerContainer() {
        return StreamMessageListenerContainer.create(redisTemplate.getConnectionFactory());
    }

     */
}
