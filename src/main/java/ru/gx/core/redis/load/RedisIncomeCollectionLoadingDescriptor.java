package ru.gx.core.redis.load;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.redis.core.RedisTemplate;
import ru.gx.core.channels.AbstractIncomeChannelHandlerDescriptor;
import ru.gx.core.channels.ChannelApiDescriptor;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessageHeader;
import ru.gx.core.redis.IncomeCollectionSortMode;

import java.security.InvalidParameterException;

/**
 * Описатель обработчика одной очереди.
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class RedisIncomeCollectionLoadingDescriptor<M extends Message<? extends MessageBody>>
        extends AbstractIncomeChannelHandlerDescriptor<M> {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">

    @NotNull
    @Getter
    private IncomeCollectionSortMode sortMode;

    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">
    public RedisIncomeCollectionLoadingDescriptor(
            @NotNull final AbstractRedisIncomeCollectionsConfiguration owner,
            @NotNull final ChannelApiDescriptor<M> api,
            @Nullable final RedisIncomeCollectionLoadingDescriptorsDefaults defaults
    ) {
        super(owner, api, defaults);
        this.sortMode = IncomeCollectionSortMode.None;
        if (defaults != null) {
            this
                    .setSortMode(defaults.getSortMode());
        }
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue", "unused"})
    @Override
    @NotNull
    public RedisIncomeCollectionLoadingDescriptor<M> init() throws InvalidParameterException {
        super.init();
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    @Override
    @NotNull
    public RedisIncomeCollectionLoadingDescriptor<M> unInit() {
        this.getOwner().internalUnregisterDescriptor(this);
        super.unInit();
        return this;
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Additional getters & setters">
    @Override
    @NotNull
    public AbstractRedisIncomeCollectionsConfiguration getOwner() {
        return (AbstractRedisIncomeCollectionsConfiguration)super.getOwner();
    }

    @NotNull
    public RedisTemplate<String, ?> getRedisTemplate() {
        if (this.getApi().getSerializeMode() == SerializeMode.JsonString) {
            return this.getOwner().getJsonStringRedisTemplate();
        }
        // this.getSerializeMode() == SerializeMode.Bytes:
        return this.getOwner().getBinaryRedisTemplate();
    }

    @NotNull
    public RedisIncomeCollectionLoadingDescriptor<M> setSortMode(@NotNull final IncomeCollectionSortMode sortMode) {
        this.checkMutable("sortMode");
        this.sortMode = sortMode;
        return this;
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}
