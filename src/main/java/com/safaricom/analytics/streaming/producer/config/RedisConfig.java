package com.safaricom.analytics.streaming.producer.config;

import com.safaricom.analytics.streaming.producer.utils.EncryptorDecryptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Autowired
    private ApplicationConfig applicationConfig;

    @Bean
    public RedisStandaloneConfiguration redisStandaloneConfiguration() {
        return new RedisStandaloneConfiguration(applicationConfig.getRedisHost(), applicationConfig.getRedisPort());
    }

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(redisStandaloneConfiguration());
        if (applicationConfig.getAuthRequired().trim().equalsIgnoreCase("true")) {
            String password = EncryptorDecryptor.decryptor(
                    "production",
                    "1.0.0", applicationConfig.getSecretKey(),
                    applicationConfig.getRedisPassword());

            lettuceConnectionFactory.setPassword(password);
        }

        return new LettuceConnectionFactory(redisStandaloneConfiguration());
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }
}
