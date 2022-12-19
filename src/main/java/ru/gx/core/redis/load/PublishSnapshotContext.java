package ru.gx.core.redis.load;

import lombok.Data;

import java.util.UUID;

@Data
public class PublishSnapshotContext {

    private UUID id;

    private boolean isLast;

    private int batchSize;
}