package ru.gx.core.redis.upload;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.channels.ChannelHandlerDescriptor;

import static lombok.AccessLevel.PROTECTED;

public abstract class AbstractRedisOutcomeCollectionsConfiguration extends AbstractChannelsConfiguration {
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
    protected AbstractRedisOutcomeCollectionsConfiguration(@NotNull final String configurationName, @NotNull RedisConnectionFactory connectionFactory) {
        super(ChannelDirection.Out, configurationName);
        this.connectionFactory = connectionFactory;
        this.jsonStringRedisTemplate = new StringRedisTemplate();
        this.binaryRedisTemplate = new RedisTemplate<>();
    }

    @Override
    protected RedisOutcomeCollectionUploadingDescriptorsDefaults createChannelDescriptorsDefaults() {
        return new RedisOutcomeCollectionUploadingDescriptorsDefaults();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeCollectionsConfiguration">
    @Override
    protected <D extends ChannelHandlerDescriptor>
    boolean allowCreateDescriptor(@NotNull Class<D> descriptorClass) {
        return RedisOutcomeCollectionUploadingDescriptor.class.isAssignableFrom(descriptorClass);
    }

    @Override
    public @NotNull RedisOutcomeCollectionUploadingDescriptorsDefaults getDescriptorsDefaults() {
        return (RedisOutcomeCollectionUploadingDescriptorsDefaults)super.getDescriptorsDefaults();
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
