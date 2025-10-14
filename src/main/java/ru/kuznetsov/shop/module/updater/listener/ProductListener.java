package ru.kuznetsov.shop.module.updater.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.ProductDto;
import ru.kuznetsov.shop.data.service.ProductService;

import static ru.kuznetsov.shop.data.common.KafkaTopics.PRODUCT_UPDATE_TOPIC;

@Component
@RequiredArgsConstructor
public class ProductListener {

    private final ProductService productService;
    private final ObjectMapper objectMapper;

    Logger logger = LoggerFactory.getLogger(ProductListener.class);

    @KafkaListener(topics = PRODUCT_UPDATE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String productDtoString) throws JsonProcessingException {
        logger.info("Updating product category {}", productDtoString);

        ProductDto productDto = objectMapper.readValue(productDtoString, ProductDto.class);

        if (productDto.getId() == null) {
            throw new RuntimeException("Product category ID is null");
        } else {
            productService.update(productDto);
        }

        logger.info("Updating finished");
    }
}
