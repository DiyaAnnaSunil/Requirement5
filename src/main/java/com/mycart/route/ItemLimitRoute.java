package com.mycart.route;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.DefaultErrorHandlerBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.component.mongodb.MongoDbConstants;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import com.mycart.model.ReviewXml;
import com.mycart.model.StoreJson;
import com.mycart.model.TrendXml;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ItemLimitRoute extends RouteBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ItemLimitRoute.class);

    @Override
    public void configure() throws Exception {
        // Parse properties to correct types
        int retryAttempts = Integer.parseInt(getContext().resolvePropertyPlaceholders("{{app.mongodb.retryAttempts}}"));
        long initialDelay = Long.parseLong(getContext().resolvePropertyPlaceholders("{{app.mongodb.initialDelay}}"));
        double backOffMultiplier = 3.0; // Hardcoded for 1 min, 3 min, 9 min approximation

        // MongoDB retry error handler
        DefaultErrorHandlerBuilder mongoErrorHandler = (DefaultErrorHandlerBuilder) new DefaultErrorHandlerBuilder()
                .maximumRedeliveries(retryAttempts) // e.g., 3 retries
                .redeliveryDelay(initialDelay) // Initial delay in ms (e.g., 60000 for 1 min)
                .useExponentialBackOff() // Enable exponential backoff
                .backOffMultiplier(backOffMultiplier) // Multiplier (approximates 1 min, 3 min, 9 min)
                .retryAttemptedLogLevel(LoggingLevel.DEBUG)
                .logRetryStackTrace(true)
                .log("Retry attempt ${exchangeProperty.CamelRedeliveryCounter} for MongoDB operation: ${exception.message}")
                .onRedelivery(exchange -> logger.debug("Redelivering MongoDB operation, attempt: ${exchangeProperty.CamelRedeliveryCounter}, error: ${exception.message}"));

        // Global exception handling
        onException(Exception.class)
                .handled(true)
                .log(LoggingLevel.ERROR, "Route failed: ${exception.message}, stacktrace: ${exception.stacktrace}, itemId: ${exchangeProperty.itemId}, currentTs: ${exchangeProperty.currentTs}")
                .stop();

        JaxbDataFormat trendXmlFormat = new JaxbDataFormat(TrendXml.class.getPackage().getName());
        JaxbDataFormat reviewXmlFormat = new JaxbDataFormat(ReviewXml.class.getPackage().getName());
        JacksonDataFormat jsonFormat = new JacksonDataFormat(StoreJson.class);

        String mongoUri = "mongodb:mongoDbComponent?database={{app.mongodb.database}}";

        from("quartz://fileExport?cron={{app.scheduler.cron}}&stateful=true")
                .routeId("fileExport")
                .bean("itemProcessor", "setCurrentTimestamp")
                .log(LoggingLevel.DEBUG, "After setCurrentTimestamp, currentTs: ${exchangeProperty.currentTs}")
                .to("direct:fetchControlRef")
                .log(LoggingLevel.DEBUG, "Before processItems, currentTs: ${exchangeProperty.currentTs}")
                .to("direct:processItems")
                .log(LoggingLevel.DEBUG, "After processItems, currentTs: ${exchangeProperty.currentTs}")
                .choice()
                .when(simple("${exchangeProperty.itemsProcessed} == true"))
                .to("direct:updateControlRef")
                .log(LoggingLevel.INFO, "Proceeding to update controlRef")
                .endChoice()
                .log(LoggingLevel.INFO, "File export completed, itemsProcessed: ${exchangeProperty.itemsProcessed}");

        from("direct:fetchControlRef")
                .routeId("fetchControlRef")
                .errorHandler(mongoErrorHandler)
                .log(LoggingLevel.DEBUG, "Before fetching controlRef")
                .setBody(constant(new Document("_id", "global")))
                .log(LoggingLevel.DEBUG, "Set query for controlRef: ${body}")
                .to(mongoUri + "&collection={{app.control.collection}}&operation=findOneByQuery&outputType=Document")
                .bean("controlRefProcessor", "fetchControlRefs")
                .log(LoggingLevel.INFO, "Fetched controlRefMap with ${exchangeProperty.controlRefMap.size()} entries");

        from("direct:processItems")
                .routeId("processItems")
                .errorHandler(mongoErrorHandler)
                .doTry()
                .bean("itemProcessor", "prepareItemQuery")
                .log(LoggingLevel.DEBUG, "After prepareItemQuery, query: ${body}")
                .setHeader(MongoDbConstants.LIMIT, constant(Integer.parseInt(getContext().resolvePropertyPlaceholders("{{app.records.processLimit}}"))))
                .to(mongoUri + "&collection={{app.item.collection}}&operation=findAll")
                .bean("itemProcessor", "validateItemList")
                .bean("itemProcessor", "filterValidItems")
                .bean("itemProcessor", "logFetchedItems")
                .choice()
                .when(simple("${body} != null && ${body.size()} > 0"))
                .bean("itemProcessor", "storeOriginalExchange")
                .setProperty("itemsProcessed", constant(true))
                .split(body())
                .log(LoggingLevel.DEBUG, "Processing item ${exchangeProperty.itemId}")
                .bean("itemProcessor", "enrichWithCategory")
                .log(LoggingLevel.DEBUG, "Executing category query for item ${exchangeProperty.itemId}, query: ${body}")
                .to("direct:fetchCategory") // Sub-route for retry
                .bean("itemProcessor", "validateCategoryResult")
                .bean("itemProcessor", "processCategoryQuery")
                .bean("itemProcessor", "mapItemData")
                .doTry()
                .bean("itemProcessor", "prepareTrendXml")
                .choice()
                .when(simple("${body} != null"))
                .marshal(trendXmlFormat)
                .setHeader("OutputFolder", constant("trend"))
                .to("direct:writeToFile")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to write trend XML for item ${exchangeProperty.itemId}: ${exception.message}")
                .end()
                .doTry()
                .bean("itemProcessor", "prepareReviewXml")
                .choice()
                .when(simple("${body} != null"))
                .marshal(reviewXmlFormat)
                .setHeader("OutputFolder", constant("review"))
                .to("direct:writeToFile")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to write review XML for item ${exchangeProperty.itemId}: ${exception.message}")
                .end()
                .doTry()
                .bean("itemProcessor", "prepareStoreJson")
                .choice()
                .when(simple("${body} != null"))
                .marshal(jsonFormat)
                .setHeader("OutputFolder", constant("store"))
                .to("direct:writeToFile")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to write store JSON for item ${exchangeProperty.itemId}: ${exception.message}")
                .end()
                .log(LoggingLevel.DEBUG, "Completed file writes for item ${exchangeProperty.itemId}")
                .end()
                .log(LoggingLevel.DEBUG, "Completed processItems split, currentTs: ${exchangeProperty.currentTs}")
                .bean("itemProcessor", "restoreOriginalExchange")
                .endChoice()
                .when(simple("${body} == null || ${body.size()} == 0"))
                .setProperty("itemsProcessed", constant(false))
                .log(LoggingLevel.INFO, "No valid items to process, skipping split")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed processing items: ${exception.message}, currentTs: ${exchangeProperty.currentTs}")
                .setProperty("itemsProcessed", constant(false))
                .end();

        // Category query sub-route with retry
        from("direct:fetchCategory")
                .routeId("fetchCategory")
                .errorHandler(mongoErrorHandler)
                .to(mongoUri + "&collection={{app.category.collection}}&operation=findOneByQuery&outputType=Document");

        from("direct:writeToFile")
                .routeId("writeToFile")
                .doTry()
                .throttle(100).timePeriodMillis(60000).asyncDelayed()
                .bean("itemProcessor", "checkFileExistence")
                .choice()
                .when(simple("${header.OutputFolder} == 'trend'"))
                .to("file://{{app.output.item-trend-analyzer}}?fileName=${header.CamelFileName}&fileExist=Override")
                .when(simple("${header.OutputFolder} == 'review'"))
                .to("file://{{app.output.item-review-aggregator}}?fileName=${header.CamelFileName}&fileExist=Override")
                .when(simple("${header.OutputFolder} == 'store'"))
                .to("file://{{app.output.storefront-app}}?fileName=${header.CamelFileName}&fileExist=Override")
                .endChoice()
                .choice()
                .when(simple("${exchangeProperty.fileExisted} == true"))
                .log(LoggingLevel.INFO, "Overwrote file: ${header.CamelFileName}")
                .when(simple("${exchangeProperty.fileExisted} == false"))
                .log(LoggingLevel.INFO, "Created new file: ${header.CamelFileName}")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to overwrite file: ${header.CamelFileName}, error: ${exception.message}")
                .end();

        from("direct:writeTrendXml")
                .routeId("writeTrendXml")
                .bean("itemProcessor", "prepareTrendXml")
                .choice()
                .when(simple("${body} != null"))
                .doTry()
                .marshal(trendXmlFormat)
                .setHeader("OutputFolder", constant("trend"))
                .to("direct:writeToFile")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to overwrite trend XML: ${header.CamelFileName}, error: ${exception.message}")
                .end()
                .endChoice();

        from("direct:writeReviewXml")
                .routeId("writeReviewXml")
                .bean("itemProcessor", "prepareReviewXml")
                .choice()
                .when(simple("${body} != null"))
                .doTry()
                .marshal(reviewXmlFormat)
                .setHeader("OutputFolder", constant("review"))
                .to("direct:writeToFile")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to overwrite review XML: ${header.CamelFileName}, error: ${exception.message}")
                .end()
                .endChoice();

        from("direct:writeStoreJson")
                .routeId("writeStoreJson")
                .bean("itemProcessor", "prepareStoreJson")
                .choice()
                .when(simple("${body} != null"))
                .doTry()
                .marshal(jsonFormat)
                .setHeader("OutputFolder", constant("store"))
                .to("direct:writeToFile")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to overwrite store JSON: ${header.CamelFileName}, error: ${exception.message}")
                .end()
                .endChoice();

        from("direct:updateControlRef")
                .routeId("updateControlRef")
                .errorHandler(mongoErrorHandler)
                .log(LoggingLevel.DEBUG, "Starting controlRef update with currentTs: ${exchangeProperty.currentTs}")
                .bean("controlRefProcessor", "updateControlRef")
                .choice()
                .when(simple("${body} != null"))
                .doTry()
                .to(mongoUri + "&collection={{app.control.collection}}&operation=save")
                .log(LoggingLevel.INFO, "controlRef updated with lastProcessTs: ${exchangeProperty.currentTs}")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to save controlRef to MongoDB: ${exception.message}")
                .end()
                .endChoice();
    }
}
