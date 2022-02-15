package ru.gx.core.redis.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import ru.gx.core.redis.load.RedisIncomeCollectionsLoader;
import ru.gx.core.redis.upload.RedisOutcomeCollectionsUploader;

@Configuration
@EnableConfigurationProperties({ConfigurationPropertiesServiceRedis.class})
public class CommonAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.redis.income-collections.standard-loader.enabled", havingValue = "true")
    public RedisIncomeCollectionsLoader redisIncomeCollectionsLoader() {
        return new RedisIncomeCollectionsLoader();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.redis.outcome-collections.standard-uploader.enabled", havingValue = "true")
    public RedisOutcomeCollectionsUploader redisOutcomeCollectionsUploader() {
        return new RedisOutcomeCollectionsUploader();
    }

    @Bean
    @ConditionalOnMissingBean
    LettuceConnectionFactory lettuceConnectionFactory(@Value("${service.redis.server}") String server, @Value("${service.redis.port}") int port) {
        return new LettuceConnectionFactory(new RedisStandaloneConfiguration(server, port));
    }
}
