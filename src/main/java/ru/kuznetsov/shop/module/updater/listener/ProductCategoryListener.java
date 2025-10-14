package ru.kuznetsov.shop.module.updater.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.ProductCategoryDto;
import ru.kuznetsov.shop.data.service.ProductCategoryService;

import static ru.kuznetsov.shop.data.common.KafkaTopics.PRODUCT_CATEGORY_UPDATE_TOPIC;

@Component
@RequiredArgsConstructor
public class ProductCategoryListener {

    private final ProductCategoryService productCategoryService;
    private final ObjectMapper objectMapper;

    Logger logger = LoggerFactory.getLogger(ProductCategoryListener.class);

    @KafkaListener(topics = PRODUCT_CATEGORY_UPDATE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String productCategoryDtoString) throws JsonProcessingException {
        logger.info("Updating product category {}", productCategoryDtoString);

        ProductCategoryDto productCategoryDto = objectMapper.readValue(productCategoryDtoString, ProductCategoryDto.class);

        if (productCategoryDto.getId() == null) {
            throw new RuntimeException("Product category ID is null");
        } else {
            productCategoryService.update(productCategoryDto);
        }

        logger.info("Updating finished");
    }
}
