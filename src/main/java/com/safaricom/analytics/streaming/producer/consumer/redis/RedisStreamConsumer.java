package com.safaricom.analytics.streaming.producer.consumer.redis;

import com.safaricom.analytics.streaming.producer.config.ApplicationConfig;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@EnableScheduling
public class RedisStreamConsumer implements StreamListener<String, MapRecord<String, String, String>>, InitializingBean, DisposableBean  {
    @Autowired
    ApplicationConfig config;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
    private Subscription subscription;
    private String consumerName;
    private String consumerGroupName;
    private String streamName;


    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        try {
            String payload = "" + message.getValue();
            redisTemplate.opsForStream().acknowledge(config.getConsumerGroupName(), message);
            System.out.println("Message" + payload);
            System.out.printf("Message % has been processed", message.getId());
        } catch (Exception ex) {
            //do something
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

        try {
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
        this.container = StreamMessageListenerContainer.create(redisTemplate.getConnectionFactory());

        this.subscription = container.receive(
                Consumer.from(consumerGroupName, consumerName),
                StreamOffset.create(streamName, ReadOffset.lastConsumed()),
                this);

        subscription.await(Duration.ofSeconds(2));
        container.start();
    }
}
