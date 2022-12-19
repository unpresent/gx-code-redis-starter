package ru.gx.core.redis.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "service.redis")
@Getter
@Setter
public class ConfigurationPropertiesServiceRedis {
    //    public static final String SERVER_DEFAULT = "localhost";
    //    public static final int PORT_DEFAULT = 6379;

    //    private String server = SERVER_DEFAULT;
    //    private int port = PORT_DEFAULT;

    @NestedConfigurationProperty
    private IncomeCollections incomeCollections;

    @NestedConfigurationProperty
    private OutcomeCollections outcomeCollections;

    @NestedConfigurationProperty
    private ReloadScheduler reloadScheduler;

    @Getter
    @Setter
    public static class IncomeCollections {
        @NestedConfigurationProperty
        private StandardLoader standardLoader = new StandardLoader();
    }

    @Getter
    @Setter
    public static class OutcomeCollections {
        @NestedConfigurationProperty
        private StandardUploader standardUploader = new StandardUploader();
    }

    @Getter
    @Setter
    public static class StandardLoader {
        private boolean enabled = true;
    }

    @Getter
    @Setter
    public static class StandardUploader {
        private boolean enabled = true;
    }

    @Getter
    @Setter
    public static class ReloadScheduler {
        private boolean enabled;
        private String cron;
    }
}
