package ru.gx.core.redis.upload;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.redis.core.RedisTemplate;
import ru.gx.core.channels.AbstractOutcomeChannelDescriptor;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.data.DataObject;
import ru.gx.core.data.DataPackage;

import java.security.InvalidParameterException;
import java.util.Properties;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class RedisOutcomeCollectionLoadingDescriptor<O extends DataObject, P extends DataPackage<O>>
        extends AbstractOutcomeChannelDescriptor<O, P> {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">


    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">

    public RedisOutcomeCollectionLoadingDescriptor(@NotNull final AbstractRedisOutcomeCollectionsConfiguration owner, @NotNull final String collection, final RedisOutcomeCollectionLoadingDescriptorsDefaults defaults) {
        super(owner, collection, defaults);
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @Override
    @NotNull
    public RedisOutcomeCollectionLoadingDescriptor<O, P> init() throws InvalidParameterException {
        super.init();
        return this;
    }

    @NotNull
    public RedisOutcomeCollectionLoadingDescriptor<O, P> unInit() {
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
        if (this.getSerializeMode() == SerializeMode.JsonString) {
            return this.getOwner().getJsonStringRedisTemplate();
        }
        // this.getSerializeMode() == SerializeMode.Bytes:
        return this.getOwner().getBinaryRedisTemplate();
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}
