package ru.gx.core.redis.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.ChannelConfigurationException;
import ru.gx.core.channels.ChannelHandlerDescriptor;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.data.DataObject;
import ru.gx.core.data.DataObjectKeyExtractor;
import ru.gx.core.data.DataPackage;
import ru.gx.core.messaging.DefaultMessagesFactory;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static lombok.AccessLevel.PROTECTED;

@SuppressWarnings({"unused", "ClassCanBeRecord"})
@Slf4j
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
    private final DefaultMessagesFactory messagesFactory;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    public RedisOutcomeCollectionsUploader(@NotNull final ObjectMapper objectMapper, @NotNull final DefaultMessagesFactory messagesFactory) {
        this.objectMapper = objectMapper;
        this.messagesFactory = messagesFactory;
    }
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
            @NotNull final Map<String, M> messages
    ) throws Exception {
        checkDescriptorIsActive(descriptor);
        internalUploadMessages(descriptor, messages);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <M extends Message<? extends MessageBody>, O extends DataObject>
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
                .<M>createByDataObject(
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
    public <M extends Message<? extends MessageBody>, O extends DataObject, P extends DataPackage<O>>
    void uploadDataPackage(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor descriptor,
            @NotNull final P objects,
            @NotNull final DataObjectKeyExtractor<O> keyExtractor
    ) throws Exception {
        uploadDataObjects(descriptor, objects.getObjects(), keyExtractor);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <M extends Message<? extends MessageBody>, O extends DataObject>
    void uploadDataObjects(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor descriptor,
            @NotNull final Collection<O> objects,
            @NotNull final DataObjectKeyExtractor<O> keyExtractor
    ) throws Exception {
        checkDescriptorIsActive(descriptor);
        final var api = descriptor.getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }

        final var map = new HashMap<String, M>();
        for (final var obj : objects) {
            final var message = getMessagesFactory()
                    .<M>createByDataObject(
                            null,
                            api.getMessageType(),
                            api.getVersion(),
                            obj,
                            null
                    );
            map.put(keyExtractor.extractKey(obj).toString(), message);
        }
        uploadMessagesWithKeys(descriptor, map);
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
            throw new ChannelConfigurationException("Collection descriptor " + descriptor.getChannelName() + " is not initialized!");
        }
        if (!descriptor.isEnabled()) {
            throw new ChannelConfigurationException("Collection descriptor " + descriptor.getChannelName() + " is not enabled!");
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
            @NotNull Map<String, M> messages
    ) throws Exception {
        final var api = descriptor.getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
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
        final var template = descriptor.getRedisTemplate();
        template.opsForHash().putAll(descriptor.getApi().getName(), serializedData);
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
