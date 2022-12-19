package ru.gx.core.redis.reload;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.gx.core.longtime.LongtimeProcess;
import ru.gx.core.longtime.LongtimeProcessService;
import ru.gx.core.messaging.MessagesPrioritizedQueue;
import ru.gx.core.redis.RedisDictionary;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(value = "service.redis.reload-scheduler.enabled", havingValue = "true")
@Service
public class DictionaryReloadService {
    @NotNull
    private final ApplicationContext applicationContext;

    @NotNull
    private final MessagesPrioritizedQueue standardMessagesPrioritizedQueue;

    @NotNull
    private final LongtimeProcessService longtimeProcessService;

    @Scheduled(cron = "${service.redis.reload-scheduler.cron}")
    public void reloadDictionaries() {
        this.standardMessagesPrioritizedQueue.pushMessage(
                0,
                new ReloadDictionaryEvent(UUID.randomUUID())
        );
    }

    @NotNull
    public LongtimeProcess startReloadProcess(@Nullable final String username) {
        final var longtimeProcess = this.longtimeProcessService.createLongtimeProcess(username);
        final var message = new ReloadDictionaryEvent(longtimeProcess.getId());
        this.standardMessagesPrioritizedQueue.pushMessage(0, message);
        return longtimeProcess;
    }

    @EventListener(ReloadDictionaryEvent.class)
    public void reload(@NotNull final ReloadDictionaryEvent event) {
        final var longtimeProcess = this.longtimeProcessService.startLongtimeProcess(event.getLongtimeProcessId());
        final var beansOfType = this.applicationContext.getBeansOfType(RedisDictionary.class);
        final var processTraceble = longtimeProcess != null;
        if (processTraceble) {
            longtimeProcess.setTotal(beansOfType.size());
        }
        for (final RedisDictionary dictionary : beansOfType.values()) {
            log.info("Reloading dictionary: {}", dictionary);
            dictionary.reload();
            if (processTraceble) {
                longtimeProcess.setCurrent(longtimeProcess.getCurrent() + 1);
            }
            log.info("Reloading dictionary: {} COMPLETE!", dictionary);
        }
        if (processTraceble) {
            this.longtimeProcessService.finishLongtimeProcess(longtimeProcess.getId());
        }
    }
}
