package ru.gx.core.redis.load;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.IncomeChannelDescriptorsDefaults;
import ru.gx.core.redis.IncomeCollectionSortMode;

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
        this.sortMode = IncomeCollectionSortMode.None;
    }
}
