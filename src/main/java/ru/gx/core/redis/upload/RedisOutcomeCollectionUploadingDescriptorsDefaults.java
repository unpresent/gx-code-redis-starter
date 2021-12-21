package ru.gx.core.redis.upload;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import ru.gx.core.channels.OutcomeChannelDescriptorsDefaults;

@SuppressWarnings("unused")
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class RedisOutcomeCollectionUploadingDescriptorsDefaults extends OutcomeChannelDescriptorsDefaults {
    protected RedisOutcomeCollectionUploadingDescriptorsDefaults() {
        super();
    }
}
