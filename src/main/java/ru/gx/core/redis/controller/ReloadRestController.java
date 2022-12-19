package ru.gx.core.redis.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.gx.core.longtime.LongtimeProcess;
import ru.gx.core.redis.reload.DictionaryReloadService;

@ConditionalOnProperty(value = "service.redis.reload-scheduler.enabled", havingValue = "true")
@RestController
@RequiredArgsConstructor
@Tag(name = "Управление словарями redis.")
@Slf4j
@RequestMapping("/dictionaries")
public class ReloadRestController {
    @NotNull
    private final DictionaryReloadService dictionaryReloadService;

    @PostMapping("/reload-all")
    @Operation(summary = "Запускает перезагрузку всех словарей redis.")
    public LongtimeProcess reloadAll() {
        log.info("START REST /reload-all");
        return this.dictionaryReloadService.startReloadProcess(null);
    }
}
