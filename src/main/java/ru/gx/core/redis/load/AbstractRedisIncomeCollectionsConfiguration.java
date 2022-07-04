package ru.gx.core.redis.load;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.channels.ChannelHandlerDescriptor;

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
    @NotNull
    private final RedisConnectionFactory connectionFactory;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractRedisIncomeCollectionsConfiguration(@NotNull final String configurationName, @NotNull final RedisConnectionFactory connectionFactory) {
        super(ChannelDirection.In, configurationName);
        this.connectionFactory = connectionFactory;
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
    protected <D extends ChannelHandlerDescriptor>
    boolean allowCreateDescriptor(@NotNull Class<D> descriptorClass) {
        return RedisIncomeCollectionLoadingDescriptor.class.isAssignableFrom(descriptorClass);
    }

    @Override
    public @NotNull RedisIncomeCollectionLoadingDescriptorsDefaults getDescriptorsDefaults() {
        return (RedisIncomeCollectionLoadingDescriptorsDefaults)super.getDescriptorsDefaults();
    }

    @Override
    public void internalRegisterDescriptor(@NotNull ChannelHandlerDescriptor descriptor) {
        super.internalRegisterDescriptor(descriptor);
        if (getBinaryRedisTemplate().getConnectionFactory() != getConnectionFactory()) {
            getBinaryRedisTemplate().setConnectionFactory(getConnectionFactory());
            getBinaryRedisTemplate().afterPropertiesSet();
        }
        if (getJsonStringRedisTemplate().getConnectionFactory() != getConnectionFactory()) {
            getJsonStringRedisTemplate().setConnectionFactory(getConnectionFactory());
            getJsonStringRedisTemplate().afterPropertiesSet();
        }
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
