package ru.gx.core.redis.reload;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import ru.gx.core.messaging.StandardMessagesPrioritizedQueue;
import ru.gx.core.redis.RedisDictionary;

@Slf4j
public class DictionaryReloadService {

    public static final ReloadDictionaryEvent reloadDictionaryEvent = new ReloadDictionaryEvent();

    private final ApplicationContext applicationContext;

    private final StandardMessagesPrioritizedQueue standardMessagesPrioritizedQueue;

    public DictionaryReloadService(
            @NotNull final ApplicationContext applicationContext,
            @NotNull final StandardMessagesPrioritizedQueue standardMessagesPrioritizedQueue
    ) {
        this.applicationContext = applicationContext;
        this.standardMessagesPrioritizedQueue = standardMessagesPrioritizedQueue;
    }

    @Scheduled(cron = "${service.redis.reload-scheduler.cron}")
    public void reloadDictionaries() {
        standardMessagesPrioritizedQueue.pushMessage(0, reloadDictionaryEvent);
    }

    @EventListener(ReloadDictionaryEvent.class)
    public void reload() {
        final var beansOfType = applicationContext.getBeansOfType(RedisDictionary.class);
        for (final RedisDictionary dictionary : beansOfType.values()) {
            log.info("Reloading dictionary: {}", dictionary);
            dictionary.reload();
            log.info("Reloading dictionary: {} COMPLETE!", dictionary);
        }
    }

}
