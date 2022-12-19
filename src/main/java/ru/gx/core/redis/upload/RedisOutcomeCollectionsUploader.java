package ru.gx.core.redis.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import ru.gx.core.channels.ChannelConfigurationException;
import ru.gx.core.channels.ChannelHandlerDescriptor;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.data.DataObject;
import ru.gx.core.data.DataObjectKeyExtractor;
import ru.gx.core.data.DataPackage;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessagesFactory;
import ru.gx.core.redis.load.PublishSnapshotContext;

import java.util.*;

import static lombok.AccessLevel.PROTECTED;

@SuppressWarnings({"unused"})
@Slf4j
@RequiredArgsConstructor
public class RedisOutcomeCollectionsUploader {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">

    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    @Getter(PROTECTED)
    @NotNull
    private final ObjectMapper objectMapper;

    /**
     * DefaultMessagesFactory требуется для создания сообщений.
     */
    @Getter(PROTECTED)
    @NotNull
    private final MessagesFactory messagesFactory;

    /**
     * StringRedisTemplate требуется для хранения временных множеств в Redis.
     */
    @Getter(PROTECTED)
    @NotNull
    private final StringRedisTemplate stringRedisTemplate;

    /**
     * Кэш ключей, которые выгрузились в Redis
     */
    private final Map<RedisOutcomeCollectionUploadingDescriptor, Set<Object>> uploadingKeysCache = new HashMap<>();
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeCollectionUploader">

    /**
     * Выгрузка одного объекта в Redis
     */
    public <M extends Message<? extends MessageBody>>
    void uploadMessage(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor descriptor,
            @NotNull final String key,
            @NotNull final M message
    ) throws Exception {
        checkDescriptorIsActive(descriptor);
        internalUploadMessage(descriptor, key, message);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <M extends Message<? extends MessageBody>>
    void uploadMessagesWithKeys(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor descriptor,
            @NotNull final Map<String, M> messages,
            boolean deleteMissed,
            @Nullable final PublishSnapshotContext context
    ) throws Exception {
        checkDescriptorIsActive(descriptor);
        internalUploadMessages(descriptor, messages, deleteMissed, context);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <O extends DataObject>
    void uploadDataObject(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor descriptor,
            @NotNull final String key,
            @NotNull final O object
    ) throws Exception {
        checkDescriptorIsActive(descriptor);
        final var api = descriptor.getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }

        final var message = getMessagesFactory()
                .createByDataObject(
                        null,
                        api.getMessageType(),
                        api.getVersion(),
                        object,
                        null
                );
        uploadMessage(descriptor, key, message);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <O extends DataObject, P extends DataPackage<O>>
    void uploadDataPackage(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor descriptor,
            @NotNull final P objects,
            @NotNull final DataObjectKeyExtractor<O> keyExtractor,
            boolean deleteMissed,
            @NotNull PublishSnapshotContext context
    ) throws Exception {
        uploadDataObjects(descriptor, objects.getObjects(), keyExtractor, deleteMissed, context);
    }

    /**
     * Выгрузка списка объектов в Redis
     *
     * @param descriptor   Описатель выгружаемого канала сообщений
     * @param objects      Список выгружаемых в пакете объектов
     * @param keyExtractor Объект, который может извлекать ключи из объектов
     * @param deleteMissed Удалять ли из Redis объекты, которых нет в списке objects
     * @param context      Контекст публикации снапшота. Может быть null, если deleteMissed == false.
     */
    public <O extends DataObject>
    void uploadDataObjects(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor descriptor,
            @NotNull final Collection<O> objects,
            @NotNull final DataObjectKeyExtractor<O> keyExtractor,
            boolean deleteMissed,
            @Nullable PublishSnapshotContext context
    ) throws Exception {
        checkDescriptorIsActive(descriptor);
        final var api = descriptor.getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }

        final var map = new HashMap<String, Message<?>>();
        for (final var obj : objects) {
            final var message = getMessagesFactory()
                    .createByDataObject(
                            null,
                            api.getMessageType(),
                            api.getVersion(),
                            obj,
                            null
                    );
            map.put(keyExtractor.extractKey(obj).toString(), message);
        }
        uploadMessagesWithKeys(descriptor, map, deleteMissed, context);
    }

    /**
     * Начало выгрузки объектов в Redis батчами
     *
     * @param descriptor Описатель канала
     */
    public void startBatchedUploadObjects(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor descriptor,
            boolean deleteMissed
    ) {
        if (deleteMissed) {
            final var keysSet = this.uploadingKeysCache.computeIfAbsent(descriptor, k -> new HashSet<>());
            keysSet.clear();
        } else {
            this.uploadingKeysCache.remove(descriptor);
        }
    }

    /**
     * Окончание выгрузки объектов в Redis батчами
     *
     * @param descriptor Описатель канала
     */
    public void finishBatchedUploadObjects(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor descriptor
    ) {
        final var uploadedKeysSet = this.uploadingKeysCache.get(descriptor);
        if (uploadedKeysSet != null) {
            final var template = descriptor.getRedisTemplate();
            final var descriptorName = descriptor.getChannelName();
            final var redisKeys = template.opsForHash().keys(descriptorName);

            redisKeys.removeAll(uploadedKeysSet);
            if (!redisKeys.isEmpty()) {
                log.info("Removing {} entries from {}", redisKeys, descriptorName);
                template.opsForHash().delete(descriptorName, redisKeys.toArray());
            }

            this.uploadingKeysCache.remove(descriptor);
        }
    }

    /**
     * Выгрузка одного батча
     */
    public <O extends DataObject>
    void batchedUploadDataObjects(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor descriptor,
            @NotNull final Collection<O> batch,
            @NotNull final DataObjectKeyExtractor<O> keyExtractor
    ) throws Exception {
        uploadDataObjects(descriptor, batch, keyExtractor, false, null);

        final var uploadedKeysSet = this.uploadingKeysCache.get(descriptor);
        if (uploadedKeysSet != null) {
            batch.forEach(item -> uploadedKeysSet.add(keyExtractor.extractKey(item)));
        }
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя логика">

    /**
     * Проверка на то, был ли инициализирован описатель.
     *
     * @param descriptor описатель.
     */
    protected void checkDescriptorIsActive(@NotNull final ChannelHandlerDescriptor descriptor) {
        if (!descriptor.isInitialized()) {
            throw new ChannelConfigurationException("Collection descriptor " + descriptor.getChannelName() + " is not" +
                    " initialized!");
        }
        if (!descriptor.isEnabled()) {
            throw new ChannelConfigurationException("Collection descriptor " + descriptor.getChannelName() + " is not" +
                    " enabled!");
        }
    }

    protected <M extends Message<? extends MessageBody>>
    void internalUploadMessage(
            @NotNull RedisOutcomeCollectionUploadingDescriptor descriptor,
            @NotNull String key,
            @NotNull M message
    ) throws Exception {
        final var api = descriptor.getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }

        Object serializedData;
        if (api.getSerializeMode() == SerializeMode.JsonString) {
            serializedData = getObjectMapper().writeValueAsString(message);
        } else {
            serializedData = getObjectMapper().writeValueAsBytes(message);
        }
        final var template = descriptor.getRedisTemplate();
        template.opsForHash().put(descriptor.getApi().getName(), key, serializedData);
    }

    protected <M extends Message<? extends MessageBody>>
    void internalUploadMessages(
            @NotNull RedisOutcomeCollectionUploadingDescriptor descriptor,
            @NotNull Map<String, M> messages,
            boolean deleteMissed,
            @Nullable PublishSnapshotContext context
    ) throws Exception {
        final var api = descriptor.getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }
        if (deleteMissed && context == null) {
            throw new NullPointerException("(deleteMissed == true) && (context is null)!");
        }

        final var serializedData = new HashMap<String, Object>();
        if (api.getSerializeMode() == SerializeMode.JsonString) {
            for (final var entry : messages.entrySet()) {
                serializedData.put(entry.getKey(), getObjectMapper().writeValueAsString(entry.getValue()));
            }
        } else {
            for (final var entry : messages.entrySet()) {
                serializedData.put(entry.getKey(), getObjectMapper().writeValueAsBytes(entry.getValue()));
            }
        }

        final var descriptorName = descriptor.getChannelName();
        final var template = descriptor.getRedisTemplate();
        final var tempSetName = deleteMissed ? getTempSetName(context, descriptorName) : null;

        if (deleteMissed) {
            final SetOperations<String, ?> stringSetOperations = template.opsForSet();
            //добавляем все ключи во временное множество, чтобы потом сравнить его с результирующим и удалить лишние
            //noinspection ToArrayCallWithZeroLengthArrayArgument
            this.stringRedisTemplate.opsForSet()
                    .add(
                            tempSetName,
                            messages.keySet().toArray(new String[messages.keySet().size()])
                    );
        }

        template.opsForHash()
                .putAll(descriptorName, serializedData);

        if (deleteMissed && context.isLast()) {
            log.info("Last batch detected, deleting old dictionary records for context: {}", context);
            int deletedEntries = 0;
            try (final var scan = template.opsForHash()
                    .scan(descriptorName, ScanOptions.scanOptions().build())) {
                //пробегаем по всему справочнику, который есть сейчас
                log.info("Starting deletion for context: {}", context);
                while (scan.hasNext()) {
                    final var next = scan.next();
                    //если в этой загрузке нет такого элемента, значит удаляем его
                    final var member = this.stringRedisTemplate.opsForSet().isMember(tempSetName, next.getKey());
                    if (Boolean.FALSE.equals(member)) {
                        log.info("Deleting dictionary record: {}", next.getValue());
                        template.opsForHash().delete(descriptorName, next.getKey());
                        deletedEntries++;
                    }
                }
            }
            //удаляем временное множество ключей
            final var tempSetDeleted = stringRedisTemplate.delete(tempSetName);
            log.info("Deletion finished for context: {}, tempSet: {}, tempSetDeleted: {}, deletedEntries: {}", context,
                    tempSetName, tempSetDeleted, deletedEntries);
        }
    }

    @NotNull
    private static String getTempSetName(@NotNull PublishSnapshotContext context, String descriptorName) {
        return descriptorName + "-" + context.getId();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
