package ru.gx.core.redis.upload;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.redis.core.RedisTemplate;
import ru.gx.core.channels.AbstractOutcomeChannelHandleDescriptor;
import ru.gx.core.channels.ChannelApiDescriptor;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessageHeader;

import java.security.InvalidParameterException;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class RedisOutcomeCollectionUploadingDescriptor<M extends Message<? extends MessageHeader, ? extends MessageBody>>
        extends AbstractOutcomeChannelHandleDescriptor<M> {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">


    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">

    public RedisOutcomeCollectionUploadingDescriptor(
            @NotNull final AbstractRedisOutcomeCollectionsConfiguration owner,
            @NotNull final ChannelApiDescriptor<M> api,
            @Nullable final RedisOutcomeCollectionUploadingDescriptorsDefaults defaults
    ) {
        super(owner, api, defaults);
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @Override
    @NotNull
    public RedisOutcomeCollectionUploadingDescriptor<M> init() throws InvalidParameterException {
        super.init();
        return this;
    }

    @NotNull
    public RedisOutcomeCollectionUploadingDescriptor<M> unInit() {
        super.unInit();
        return this;
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Additional getters & setters">
    @Override
    @NotNull
    public AbstractRedisOutcomeCollectionsConfiguration getOwner() {
        return (AbstractRedisOutcomeCollectionsConfiguration)super.getOwner();
    }

    @NotNull
    public RedisTemplate<String, ?> getRedisTemplate() {
        if (this.getApi().getSerializeMode() == SerializeMode.JsonString) {
            return this.getOwner().getJsonStringRedisTemplate();
        }
        // this.getSerializeMode() == SerializeMode.Bytes:
        return this.getOwner().getBinaryRedisTemplate();
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Messages generating">

    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}
