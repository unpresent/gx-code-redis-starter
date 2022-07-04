package ru.gx.core.redis.upload;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.redis.core.RedisTemplate;
import ru.gx.core.channels.*;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;

import java.security.InvalidParameterException;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class RedisOutcomeCollectionUploadingDescriptor
        extends AbstractOutcomeChannelHandlerDescriptor {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">


    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">

    protected RedisOutcomeCollectionUploadingDescriptor(
            @NotNull final AbstractRedisOutcomeCollectionsConfiguration owner,
            @NotNull final ChannelApiDescriptor<? extends Message<? extends MessageBody>> api,
            @Nullable final RedisOutcomeCollectionUploadingDescriptorsDefaults defaults
    ) {
        super(owner, api, defaults);
    }

    protected RedisOutcomeCollectionUploadingDescriptor(
            @NotNull final ChannelsConfiguration owner,
            @NotNull final String channelName,
            @Nullable final OutcomeChannelDescriptorsDefaults defaults) {
        super(owner, channelName, defaults);
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @Override
    @NotNull
    public RedisOutcomeCollectionUploadingDescriptor init() throws InvalidParameterException {
        super.init();
        return this;
    }

    @NotNull
    public RedisOutcomeCollectionUploadingDescriptor unInit() {
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
        final var api = getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }

        if (api.getSerializeMode() == SerializeMode.JsonString) {
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
