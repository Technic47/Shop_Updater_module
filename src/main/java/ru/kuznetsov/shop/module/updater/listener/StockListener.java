package ru.kuznetsov.shop.module.updater.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.StockDto;
import ru.kuznetsov.shop.data.service.StockService;

import static ru.kuznetsov.shop.data.common.KafkaTopics.STOCK_UPDATE_TOPIC;

@Component
@RequiredArgsConstructor
public class StockListener {

    private final StockService stockService;
    private final ObjectMapper objectMapper;

    Logger logger = LoggerFactory.getLogger(StockListener.class);

    @KafkaListener(topics = STOCK_UPDATE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String stockDtoString) throws JsonProcessingException {
        logger.info("Updating stock {}", stockDtoString);

        StockDto stockDto = objectMapper.readValue(stockDtoString, StockDto.class);

        if (stockDto.getId() == null) {
            throw new RuntimeException("Stock ID is null");
        } else {
            stockService.update(stockDto);
        }

        logger.info("Updating finished");
    }
}
