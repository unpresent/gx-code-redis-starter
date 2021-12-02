package ru.gx.core.redis.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.core.channels.ChannelConfigurationException;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.data.DataMemoryRepository;
import ru.gx.core.data.DataObject;
import ru.gx.core.data.DataPackage;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static lombok.AccessLevel.PROTECTED;

public class RedisOutcomeCollectionsUploader {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    @NotNull
    private ObjectMapper objectMapper;
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    public RedisOutcomeCollectionsUploader() {
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeCollectionUploader">

    /**
     * Выгрузка одного объекта в Redis
     */
    public <O extends DataObject, P extends DataPackage<O>>
    void uploadObject(
            @NotNull final RedisOutcomeCollectionLoadingDescriptor<O, P> descriptor,
            @NotNull final String key,
            @NotNull final O object
    ) throws Exception {
        checkDescriptorIsInitialized(descriptor);
        internalUploadObject(descriptor, key, object);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <O extends DataObject, P extends DataPackage<O>>
    void uploadObjectsWithKeys(
            @NotNull final RedisOutcomeCollectionLoadingDescriptor<O, P> descriptor,
            @NotNull final Map<String, O> objects
    ) throws Exception {
        checkDescriptorIsInitialized(descriptor);
        internalUploadObjects(descriptor, objects);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <O extends DataObject, P extends DataPackage<O>>
    void uploadDataPackage(
            @NotNull final RedisOutcomeCollectionLoadingDescriptor<O, P> descriptor,
            @NotNull final P objects,
            @NotNull final DataMemoryRepository<O, P> memoryRepository
    ) throws Exception {
        uploadDataObjects(descriptor, objects.getObjects(), memoryRepository);
    }

    /**
     * Выгрузка списка объектов в Redis
     */
    public <O extends DataObject, P extends DataPackage<O>>
    void uploadDataObjects(
            @NotNull final RedisOutcomeCollectionLoadingDescriptor<O, P> descriptor,
            @NotNull final Collection<O> objects,
            @NotNull final DataMemoryRepository<O, P> memoryRepository
    ) throws Exception {
        checkDescriptorIsInitialized(descriptor);
        final var map = new HashMap<String, O>();
        for (final var obj : objects) {
            map.put(memoryRepository.extractKey(obj).toString(), obj);
        }
        internalUploadObjects(descriptor, map);
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя логика">
    /**
     * Проверка на то, был ли инициализирован описатель.
     *
     * @param descriptor описатель.
     */
    protected void checkDescriptorIsInitialized(@NotNull final RedisOutcomeCollectionLoadingDescriptor<?, ?> descriptor) {
        if (!descriptor.isInitialized()) {
            throw new ChannelConfigurationException("Collection descriptor " + descriptor.getName() + " is not initialized!");
        }
    }

    protected <O extends DataObject, P extends DataPackage<O>>
    void internalUploadObject(
            @NotNull RedisOutcomeCollectionLoadingDescriptor<O, P> descriptor,
            @NotNull String key,
            @NotNull DataObject data
    ) throws Exception {

        Object serializedData;
        if (descriptor.getSerializeMode() == SerializeMode.JsonString) {
            serializedData = this.objectMapper.writeValueAsString(data);
        } else {
            serializedData = this.objectMapper.writeValueAsBytes(data);
        }
        final var template = descriptor.getRedisTemplate();
        template.opsForHash().put(descriptor.getName(), key, serializedData);
    }

    protected <O extends DataObject, P extends DataPackage<O>>
    void internalUploadObjects(
            @NotNull RedisOutcomeCollectionLoadingDescriptor<O, P> descriptor,
            @NotNull Map<String, O> objects
    ) throws Exception {

        final var serializedData = new HashMap<String, Object>();
        if (descriptor.getSerializeMode() == SerializeMode.JsonString) {
            for (final var entry : objects.entrySet()) {
                serializedData.put(entry.getKey(), this.objectMapper.writeValueAsString(entry.getValue()));
            }
        } else {
            for (final var entry : objects.entrySet()) {
                serializedData.put(entry.getKey(), this.objectMapper.writeValueAsBytes(entry.getValue()));
            }
        }
        final var template = descriptor.getRedisTemplate();
        template.opsForHash().putAll(descriptor.getName(), serializedData);
    }

    /**
     * Создание нового экземпляра пакета объектов.
     *
     * @param descriptor описатель.
     * @return пакет объектов.
     */
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    P createPackage(@NotNull final RedisOutcomeCollectionLoadingDescriptor<O, P> descriptor) throws Exception {
        final var packageClass = descriptor.getDataPackageClass();
        if (packageClass != null) {
            final var constructor = packageClass.getConstructor();
            return constructor.newInstance();
        } else {
            throw new Exception("Can't create DataPackage!");
        }
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
