package ru.gx.core.redis.load;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

@Getter
@ToString
@Accessors(chain = true)
@RequiredArgsConstructor
public class PublishSnapshotContext {
    @NotNull
    private final UUID id;

    @Setter
    private boolean isLast;

    @Setter
    private int batchSize;
}