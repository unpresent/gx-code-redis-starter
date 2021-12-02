package ru.gx.core.redis.load;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelConfigurationException;
import ru.gx.core.channels.ChannelDescriptor;
import ru.gx.core.channels.ChannelDirection;

import static lombok.AccessLevel.PROTECTED;

public abstract class AbstractRedisIncomeCollectionsConfiguration extends AbstractChannelsConfiguration {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    @Getter
    @NotNull
    private final StringRedisTemplate jsonStringRedisTemplate;

    @Getter
    @NotNull
    private final RedisTemplate<String, byte[]> binaryRedisTemplate;

    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private RedisConnectionFactory connectionFactory;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractRedisIncomeCollectionsConfiguration(@NotNull final String configurationName) {
        super(ChannelDirection.In, configurationName);
        this.jsonStringRedisTemplate = new StringRedisTemplate();
        this.binaryRedisTemplate = new RedisTemplate<>();
    }

    @Override
    protected RedisIncomeCollectionLoadingDescriptorsDefaults createChannelDescriptorsDefaults() {
        return new RedisIncomeCollectionLoadingDescriptorsDefaults();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeCollectionsConfiguration">
    @Override
    protected boolean allowCreateDescriptor(@NotNull Class<? extends ChannelDescriptor> descriptorClass) {
        return RedisIncomeCollectionLoadingDescriptor.class.isAssignableFrom(descriptorClass);
    }

    @Override
    public @NotNull RedisIncomeCollectionLoadingDescriptorsDefaults getDescriptorsDefaults() {
        return (RedisIncomeCollectionLoadingDescriptorsDefaults)super.getDescriptorsDefaults();
    }

    @Override
    public void internalRegisterDescriptor(@NotNull ChannelDescriptor descriptor) {
        super.internalRegisterDescriptor(descriptor);
        if (this.connectionFactory == null) {
            throw new ChannelConfigurationException("Redis Connection factory isn't initialized!");
        }
        if (this.binaryRedisTemplate.getConnectionFactory() != this.connectionFactory) {
            this.binaryRedisTemplate.setConnectionFactory(this.connectionFactory);
            this.binaryRedisTemplate.afterPropertiesSet();
        }
        if (this.jsonStringRedisTemplate.getConnectionFactory() != this.connectionFactory) {
            this.jsonStringRedisTemplate.setConnectionFactory(this.connectionFactory);
            this.jsonStringRedisTemplate.afterPropertiesSet();
        }
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
