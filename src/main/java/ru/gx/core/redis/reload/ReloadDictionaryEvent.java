package ru.gx.core.redis.reload;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.longtime.LongtimeProcessEvent;

import java.util.UUID;

public class ReloadDictionaryEvent extends LongtimeProcessEvent {
    public ReloadDictionaryEvent(@NotNull UUID longtimeProcessId) {
        super(longtimeProcessId);
    }
}
