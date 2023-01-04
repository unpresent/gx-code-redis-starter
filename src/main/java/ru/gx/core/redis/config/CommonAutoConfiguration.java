package ru.gx.core.redis.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import ru.gx.core.api.rest.AdviceController;
import ru.gx.core.api.rest.RedirectController;
import ru.gx.core.messaging.MessagesFactory;
import ru.gx.core.messaging.MessagesPrioritizedQueue;
import ru.gx.core.redis.load.RedisIncomeCollectionsLoader;
import ru.gx.core.redis.upload.RedisOutcomeCollectionsUploader;

@SuppressWarnings("unused")
@Configuration
@EnableConfigurationProperties({ConfigurationPropertiesServiceRedis.class})
@EnableScheduling
@ComponentScan("ru.gx.core.redis")
public class CommonAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.redis.income-collections.standard-loader.enabled", havingValue = "true")
    public RedisIncomeCollectionsLoader redisIncomeCollectionsLoader(
            @NotNull final ApplicationEventPublisher eventPublisher,
            @NotNull final ObjectMapper objectMapper,
            @NotNull final MessagesPrioritizedQueue messagesPrioritizedQueue
    ) {
        return new RedisIncomeCollectionsLoader(eventPublisher, objectMapper, messagesPrioritizedQueue);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.redis.outcome-collections.standard-uploader.enabled", havingValue = "true")
    public RedisOutcomeCollectionsUploader redisOutcomeCollectionsUploader(
            @NotNull final ObjectMapper objectMapper,
            @NotNull final MessagesFactory messagesFactory,
            @NotNull final StringRedisTemplate stringRedisTemplate
    ) {
        return new RedisOutcomeCollectionsUploader(objectMapper, messagesFactory, stringRedisTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "rest-service-api.redirect-controller-enabled", havingValue = "true")
    public RedirectController redirectController() {
        return new RedirectController();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "rest-service-api.advice-controller-enabled", havingValue = "true")
    public AdviceController adviceController(
            @NotNull final ObjectMapper objectMapper
    ) {
        return new AdviceController(objectMapper);
    }
}
