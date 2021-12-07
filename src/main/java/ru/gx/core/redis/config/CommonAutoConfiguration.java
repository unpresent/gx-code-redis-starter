package ru.gx.core.redis.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import ru.gx.core.redis.load.BootstrapRedisIncomeCollectionsConfiguration;
import ru.gx.core.redis.load.RedisIncomeCollectionsLoader;
import ru.gx.core.redis.load.SimpleRedisIncomeCollectionsConfiguration;
import ru.gx.core.redis.upload.RedisOutcomeCollectionsUploader;
import ru.gx.core.redis.upload.SimpleRedisOutcomeCollectionsConfiguration;

@Configuration
@EnableConfigurationProperties({ConfigurationPropertiesServiceRedis.class})
public class CommonAutoConfiguration {
    private static final String SIMPLE_INCOME_CONFIG_PREFIX = ":in:simple-redis";
    private static final String BOOSTRAP_INCOME_CONFIG_PREFIX = ":in:bootstrap-redis";
    private static final String SIMPLE_OUTCOME_CONFIG_PREFIX = ":out:simple-redis";

    @Value("${service.name}")
    private String serviceName;

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.redis.income-collections.simple-configuration.enabled", havingValue = "true")
    public SimpleRedisIncomeCollectionsConfiguration simpleRedisIncomeCollectionsConfiguration() {
        return new SimpleRedisIncomeCollectionsConfiguration(this.serviceName + SIMPLE_INCOME_CONFIG_PREFIX);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.redis.income-collections.bootstrap-configuration.enabled", havingValue = "true")
    public BootstrapRedisIncomeCollectionsConfiguration bootstrapRedisIncomeCollectionsConfiguration() {
        return new BootstrapRedisIncomeCollectionsConfiguration(this.serviceName + BOOSTRAP_INCOME_CONFIG_PREFIX);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.redis.income-collections.standard-loader.enabled", havingValue = "true")
    public RedisIncomeCollectionsLoader redisIncomeCollectionsLoader() {
        return new RedisIncomeCollectionsLoader();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.redis.outcome-collections.simple-configuration.enabled", havingValue = "true")
    public SimpleRedisOutcomeCollectionsConfiguration simpleRedisOutcomeCollectionsConfiguration() {
        return new SimpleRedisOutcomeCollectionsConfiguration(this.serviceName + SIMPLE_OUTCOME_CONFIG_PREFIX);
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
