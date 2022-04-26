package ru.gx.core.redis.upload;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.channels.ChannelHandlerDescriptor;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessageHeader;

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
    protected <M extends Message<? extends MessageHeader, ? extends MessageBody>, D extends ChannelHandlerDescriptor<M>>
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
