package ru.gx.core.redis.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
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
import ru.gx.core.messaging.MessageHeader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static lombok.AccessLevel.PROTECTED;

@SuppressWarnings("unused")
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
    public <M extends Message<? extends MessageHeader, ? extends MessageBody>>
    void uploadMessage(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor<M> descriptor,
            @NotNull final String key,
            @NotNull final M message
    ) throws Exception {
        checkDescriptorIsActive(descriptor);
        internalUploadMessage(descriptor, key, message);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <M extends Message<? extends MessageHeader, ? extends MessageBody>>
    void uploadMessagesWithKeys(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor<M> descriptor,
            @NotNull final Map<String, M> messages
    ) throws Exception {
        checkDescriptorIsActive(descriptor);
        internalUploadMessages(descriptor, messages);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <M extends Message<? extends MessageHeader, ? extends MessageBody>, O extends DataObject>
    void uploadDataObject(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor<M> descriptor,
            @NotNull final String key,
            @NotNull final O object
    ) throws Exception {
        checkDescriptorIsActive(descriptor);
        final var message = this.messagesFactory
                .<M>createByDataObject(
                        null,
                        descriptor.getApi().getMessageType(),
                        descriptor.getApi().getVersion(),
                        object,
                        null
                );
        uploadMessage(descriptor, key, message);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <M extends Message<? extends MessageHeader, ? extends MessageBody>, O extends DataObject, P extends DataPackage<O>>
    void uploadDataPackage(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor<M> descriptor,
            @NotNull final P objects,
            @NotNull final DataObjectKeyExtractor<O> keyExtractor
    ) throws Exception {
        uploadDataObjects(descriptor, objects.getObjects(), keyExtractor);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <M extends Message<? extends MessageHeader, ? extends MessageBody>, O extends DataObject>
    void uploadDataObjects(
            @NotNull final RedisOutcomeCollectionUploadingDescriptor<M> descriptor,
            @NotNull final Collection<O> objects,
            @NotNull final DataObjectKeyExtractor<O> keyExtractor
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        final var map = new HashMap<String, M>();
        for (final var obj : objects) {
            final var message = this.messagesFactory
                    .<M>createByDataObject(
                            null,
                            descriptor.getApi().getMessageType(),
                            descriptor.getApi().getVersion(),
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
    protected void checkDescriptorIsActive(@NotNull final ChannelHandlerDescriptor<?> descriptor) {
        if (!descriptor.isInitialized()) {
            throw new ChannelConfigurationException("Collection descriptor " + descriptor.getApi().getName() + " is not initialized!");
        }
        if (!descriptor.isEnabled()) {
            throw new ChannelConfigurationException("Collection descriptor " + descriptor.getApi().getName() + " is not enabled!");
        }
    }

    protected <M extends Message<? extends MessageHeader, ? extends MessageBody>>
    void internalUploadMessage(
            @NotNull RedisOutcomeCollectionUploadingDescriptor<M> descriptor,
            @NotNull String key,
            @NotNull M message
    ) throws Exception {

        Object serializedData;
        if (descriptor.getApi().getSerializeMode() == SerializeMode.JsonString) {
            serializedData = this.objectMapper.writeValueAsString(message);
        } else {
            serializedData = this.objectMapper.writeValueAsBytes(message);
        }
        final var template = descriptor.getRedisTemplate();
        template.opsForHash().put(descriptor.getApi().getName(), key, serializedData);
    }

    protected <M extends Message<? extends MessageHeader, ? extends MessageBody>>
    void internalUploadMessages(
            @NotNull RedisOutcomeCollectionUploadingDescriptor<M> descriptor,
            @NotNull Map<String, M> messages
    ) throws Exception {

        final var serializedData = new HashMap<String, Object>();
        if (descriptor.getApi().getSerializeMode() == SerializeMode.JsonString) {
            for (final var entry : messages.entrySet()) {
                serializedData.put(entry.getKey(), this.objectMapper.writeValueAsString(entry.getValue()));
            }
        } else {
            for (final var entry : messages.entrySet()) {
                serializedData.put(entry.getKey(), this.objectMapper.writeValueAsBytes(entry.getValue()));
            }
        }
        final var template = descriptor.getRedisTemplate();
        template.opsForHash().putAll(descriptor.getApi().getName(), serializedData);
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
