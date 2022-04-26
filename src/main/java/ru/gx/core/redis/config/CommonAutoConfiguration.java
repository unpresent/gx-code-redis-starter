package ru.gx.core.redis.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import ru.gx.core.messaging.DefaultMessagesFactory;
import ru.gx.core.messaging.MessagesPrioritizedQueue;
import ru.gx.core.redis.load.RedisIncomeCollectionsLoader;
import ru.gx.core.redis.upload.RedisOutcomeCollectionsUploader;

@Configuration
@EnableConfigurationProperties({ConfigurationPropertiesServiceRedis.class})
public class CommonAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.redis.income-collections.standard-loader.enabled", havingValue = "true")
    @Autowired
    public RedisIncomeCollectionsLoader redisIncomeCollectionsLoader(
            @NotNull final ApplicationEventPublisher eventPublisher,
            @NotNull final ObjectMapper objectMapper,
            @NotNull final MessagesPrioritizedQueue eventsQueue
    ) {
        return new RedisIncomeCollectionsLoader(eventPublisher, objectMapper, eventsQueue);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.redis.outcome-collections.standard-uploader.enabled", havingValue = "true")
    @Autowired
    public RedisOutcomeCollectionsUploader redisOutcomeCollectionsUploader(
            @NotNull final ObjectMapper objectMapper,
            @NotNull final DefaultMessagesFactory messagesFactory
    ) {
        return new RedisOutcomeCollectionsUploader(objectMapper, messagesFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    LettuceConnectionFactory lettuceConnectionFactory(
            @Value("${service.redis.server}") String server,
            @Value("${service.redis.port}") int port
    ) {
        return new LettuceConnectionFactory(new RedisStandaloneConfiguration(server, port));
    }
}
