package com.mycart.processor;

import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("mongoRetryHelper")
public class MongoRetryHelper {

    private static final Logger logger = LoggerFactory.getLogger(MongoRetryHelper.class);

    @Value("${app.retry.initialDelay}")
    private long initialDelay;

    @Value("${app.retry.backOffMultiplier}")
    private int backOffMultiplier;

    public long getBackoffDelay(Exchange exchange) {
        Integer retryCount = exchange.getProperty("MongoRetryCount", Integer.class);
        if (retryCount == null) retryCount = 1;

        long delay = initialDelay * (long) Math.pow(backOffMultiplier, retryCount - 1);
        logger.info("Retry #{} - applying delay of {} ms", retryCount, delay);

        // Safely increment the retry count in exchange property
        exchange.setProperty("MongoRetryCount", retryCount + 1);

        return delay;
    }
}
