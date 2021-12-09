package ru.gx.core.redis.load;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.AbstractChannelDescriptorsDefaults;
import ru.gx.core.channels.ChannelMessageMode;
import ru.gx.core.channels.IncomeChannelDescriptorsDefaults;
import ru.gx.core.redis.IncomeCollectionSortMode;

import java.security.InvalidParameterException;

@Getter
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class RedisIncomeCollectionLoadingDescriptorsDefaults extends IncomeChannelDescriptorsDefaults {

    @Setter
    @NotNull
    private IncomeCollectionSortMode sortMode;

    protected RedisIncomeCollectionLoadingDescriptorsDefaults() {
        super();
        this.setMessageMode(ChannelMessageMode.Object);
        this.sortMode = IncomeCollectionSortMode.None;
    }

    @Override
    public AbstractChannelDescriptorsDefaults setMessageMode(@NotNull final ChannelMessageMode messageMode) {
        if (messageMode != ChannelMessageMode.Object) {
            throw new InvalidParameterException("Only ChannelMessageMode.Object supported by Redis!");
        }
        return super.setMessageMode(messageMode);
    }
}
