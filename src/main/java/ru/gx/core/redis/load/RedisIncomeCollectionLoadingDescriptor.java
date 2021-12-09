package ru.gx.core.redis.load;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.redis.core.RedisTemplate;
import ru.gx.core.channels.AbstractChannelDescriptor;
import ru.gx.core.channels.AbstractIncomeChannelDescriptor;
import ru.gx.core.channels.ChannelMessageMode;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.data.DataObject;
import ru.gx.core.data.DataPackage;
import ru.gx.core.redis.IncomeCollectionSortMode;

import java.security.InvalidParameterException;

/**
 * Описатель обработчика одной очереди.
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class RedisIncomeCollectionLoadingDescriptor<O extends DataObject, P extends DataPackage<O>>
        extends AbstractIncomeChannelDescriptor<O, P> {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">

    @NotNull
    @Getter
    private IncomeCollectionSortMode sortMode;

    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">
    public RedisIncomeCollectionLoadingDescriptor(@NotNull final AbstractRedisIncomeCollectionsConfiguration owner, @NotNull final String collectionName, @Nullable final RedisIncomeCollectionLoadingDescriptorsDefaults defaults) {
        super(owner, collectionName, defaults);
        this.sortMode = IncomeCollectionSortMode.None;
        this.setMessageMode(ChannelMessageMode.Object);
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
    public RedisIncomeCollectionLoadingDescriptor<O, P> init() throws InvalidParameterException {
        super.init();
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    @Override
    @NotNull
    public RedisIncomeCollectionLoadingDescriptor<O, P> unInit() {
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
        if (this.getSerializeMode() == SerializeMode.JsonString) {
            return this.getOwner().getJsonStringRedisTemplate();
        }
        // this.getSerializeMode() == SerializeMode.Bytes:
        return this.getOwner().getBinaryRedisTemplate();
    }

    @SuppressWarnings("unchecked")
    @Override
    public @NotNull RedisIncomeCollectionLoadingDescriptor<O, P> setMessageMode(@NotNull final ChannelMessageMode messageMode) {
        if (messageMode != ChannelMessageMode.Object) {
            throw new InvalidParameterException("Only ChannelMessageMode.Object supported by Redis!");
        }
        return (RedisIncomeCollectionLoadingDescriptor<O, P>)super.setMessageMode(messageMode);
    }

    @NotNull
    public RedisIncomeCollectionLoadingDescriptor<O, P> setSortMode(@NotNull final IncomeCollectionSortMode sortMode) {
        this.checkChangeable("sortMode");
        this.sortMode = sortMode;
        return this;
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}
