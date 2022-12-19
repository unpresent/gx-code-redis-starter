package ru.gx.core.redis.load;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.ApplicationEventPublisher;
import ru.gx.core.channels.ChannelConfigurationException;
import ru.gx.core.channels.IncomeDataProcessType;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessagesPrioritizedQueue;
import ru.gx.core.redis.IncomeCollectionSortMode;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import static lombok.AccessLevel.PROTECTED;

/**
 * Базовая реализация загрузчика, который упрощает задачу чтения данных из очереди и десериалиазции их в объекты.
 */
@SuppressWarnings({"unused", "ClassCanBeRecord"})
@Slf4j
@RequiredArgsConstructor
public class RedisIncomeCollectionsLoader {
    private final static int MAX_SLEEP_MS = 64;

    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">

    /**
     * Требуется для отправки сообщений в обработку.
     */
    @Getter(PROTECTED)
    @NotNull
    private final ApplicationEventPublisher eventPublisher;

    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    @Getter(PROTECTED)
    @NotNull
    private final ObjectMapper objectMapper;

    /**
     * Требуется для отправки сообщений в обработку.
     */
    @Getter(PROTECTED)
    @NotNull
    private final MessagesPrioritizedQueue eventsQueue;
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeCollectionsLoader">

    /**
     * Загрузка и обработка данных по списку топиков по конфигурации.
     *
     * @param descriptor Описатель загрузки из Топика.
     * @return Список загруженных объектов.
     */
    public int processByCollection(@NotNull final RedisIncomeCollectionLoadingDescriptor descriptor) {
        checkDescriptorIsActive(descriptor);
        return internalProcessDescriptor(descriptor);
    }

    /**
     * Чтение объектов из очередей в порядке определенной в конфигурации.
     *
     * @return Map-а, в которой для каждого дескриптора указан список загруженных объектов.
     */
    @NotNull
    public Map<RedisIncomeCollectionLoadingDescriptor, Integer>
    processAllCollections(@NotNull final AbstractRedisIncomeCollectionsConfiguration configuration) throws InvalidParameterException {
        final var pCount = configuration.prioritiesCount();
        final var result = new HashMap<RedisIncomeCollectionLoadingDescriptor, Integer>();
        for (int p = 0; p < pCount; p++) {
            final var collectionDescriptors = configuration.getByPriority(p);
            if (collectionDescriptors == null) {
                throw new ChannelConfigurationException("Invalid null value getByPriority(" + p + ")");
            }
            for (var descriptor : collectionDescriptors) {
                if (descriptor.isEnabled()) {
                    if (descriptor instanceof final RedisIncomeCollectionLoadingDescriptor redisDescriptor) {
                        log.debug("Loading working data from collection: {}", descriptor.getChannelName());
                        final var eventsCount = processByCollection(redisDescriptor);
                        result.put(redisDescriptor, eventsCount);
                        log.debug("Loaded working data from collection. Events: {}", redisDescriptor.getChannelName());
                    } else {
                        throw new ChannelConfigurationException("Invalid class of descriptor " + descriptor.getChannelName());
                    }
                }
            }
        }
        return result;
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя реализация">

    /**
     * Проверка описателя на то, что прошла инициализация. Работать с неинициализированным описателем нельзя.
     *
     * @param descriptor описатель, который проверяем.
     */
    protected void checkDescriptorIsActive(@NotNull final RedisIncomeCollectionLoadingDescriptor descriptor) {
        if (!descriptor.isInitialized()) {
            throw new ChannelConfigurationException("Channel descriptor " + descriptor.getChannelName() + " is not initialized!");
        }
        if (!descriptor.isEnabled()) {
            throw new ChannelConfigurationException("Channel descriptor " + descriptor.getChannelName() + " is not enabled!");
        }
    }

    /**
     * Обработка входящих данных для указанного канала.
     *
     * @param descriptor Описатель загрузки из Топика.
     * @return Список событий на обработку.
     */
    @SneakyThrows
    protected int internalProcessDescriptor(@NotNull final RedisIncomeCollectionLoadingDescriptor descriptor) {
        // TODO: Добавить сбор статистики
        final var records = internalLoadAll(descriptor);

        var messagesCount = 0;

        if (descriptor.getSortMode() == IncomeCollectionSortMode.None) {
            for (var rec : records.values()) {
                internalProcessRecord(descriptor, rec);
                messagesCount++;
            }
        } else {
            Object[] sortedKeys;
            if (descriptor.getSortMode() == IncomeCollectionSortMode.KeyAsc) {
                sortedKeys = records.keySet().stream().sorted().toArray();
            } else {
                sortedKeys = records.keySet().stream().sorted((o1, o2) -> -o1.toString().compareTo(o2.toString())).toArray();
            }
            for (var key : sortedKeys) {
                final var rec = records.get(key);
                internalProcessRecord(descriptor, rec);
                messagesCount++;
            }
        }

        return messagesCount;
    }

    /**
     * Данный метод создает объект-событие, сохраняя в него данные.<br/>
     * Если в описателе канала {@code descriptor} указано, что обработка должна быть немедленной ({@link RedisIncomeCollectionLoadingDescriptor#getProcessType()}),
     * то событие бросается непосредственно в этом потоке.<br/>
     * Иначе пытаемся бросить событие в {@code eventsQueue}.
     * Перед вызовом {@link MessagesPrioritizedQueue#pushMessage(int, Message)} сначала проверяем, можно ли в очередь положить событие:
     * {@link MessagesPrioritizedQueue#allowPush()}
     *
     * @param descriptor Описатель канала.
     * @param record     Запись, полученная из Redis.
     */
    @SuppressWarnings({"BusyWait", "unchecked"})
    @SneakyThrows({InterruptedException.class, JsonProcessingException.class, IOException.class})
    protected void internalProcessRecord(@NotNull final RedisIncomeCollectionLoadingDescriptor descriptor, @NotNull final Object record) {
        Message<MessageBody> message;
        final var api = descriptor.getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }

        if (descriptor.getApi().getSerializeMode() == SerializeMode.JsonString) {
            final var strValue = (String) record;
            message = (Message<MessageBody>) getObjectMapper().readValue(strValue, descriptor.getApi().getMessageClass());
        } else {
            final var strValue = (byte[]) record;
            message = (Message<MessageBody>) getObjectMapper().readValue(strValue, descriptor.getApi().getMessageClass());
        }
        message.setChannelDescriptor(descriptor);

        if (descriptor.getProcessType() == IncomeDataProcessType.Immediate) {
            // Если обработка непосредственная, то прям в этом потоке вызываем обработчик(и) события.
            getEventPublisher().publishEvent(message);
        } else {
            // Перед тем, как положить в очередь требуется дождаться "зеленного сигнала".
            var sleepMs = 1;
            while (!getEventsQueue().allowPush()) {
                Thread.sleep(sleepMs);
                if (sleepMs < MAX_SLEEP_MS) {
                    sleepMs *= 2;
                }
            }
            // Собственно только теперь бросаем событие в очередь
            getEventsQueue().pushMessage(descriptor.getPriority(), message);
        }
    }

    /**
     * Получение данных из Redis-а всех объектов коллекции.
     *
     * @param descriptor Описатель загрузки из Коллекции.
     * @return Записи Коллекции.
     */
    @Nullable
    protected Object internalLoad(@NotNull final RedisIncomeCollectionLoadingDescriptor descriptor, @NotNull final String key) {
        final var template = descriptor.getRedisTemplate();
        final var record = template.opsForHash().get(descriptor.getChannelName(), key);
        log.debug("Collection: {}; loaded 1 record by key: {}", descriptor.getChannelName(), key);
        return record;
    }

    /**
     * Получение данных из Redis-а всех объектов коллекции.
     *
     * @param descriptor Описатель загрузки из Коллекции.
     * @return Записи Коллекции.
     */
    @NotNull
    protected Map<Object, Object> internalLoadAll(@NotNull final RedisIncomeCollectionLoadingDescriptor descriptor) {
        final var template = descriptor.getRedisTemplate();
        final var records = template.opsForHash().entries(descriptor.getChannelName());
        log.debug("Collection: {}; loaded: {} records", descriptor.getChannelName(), records.keySet().size());
        return records;
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
