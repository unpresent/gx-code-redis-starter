package ru.gx.core.redis.upload;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.AbstractChannelDescriptorsDefaults;
import ru.gx.core.channels.ChannelMessageMode;
import ru.gx.core.channels.OutcomeChannelDescriptorsDefaults;

import java.security.InvalidParameterException;
import java.util.Properties;

@SuppressWarnings("unused")
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class RedisOutcomeCollectionLoadingDescriptorsDefaults extends OutcomeChannelDescriptorsDefaults {
    protected RedisOutcomeCollectionLoadingDescriptorsDefaults() {
        super();
    }

    @Override
    public AbstractChannelDescriptorsDefaults setMessageMode(@NotNull final ChannelMessageMode messageMode) {
        if (messageMode != ChannelMessageMode.Object) {
            throw new InvalidParameterException("Only ChannelMessageMode.Object supported by Redis!");
        }
        return super.setMessageMode(messageMode);
    }
}
