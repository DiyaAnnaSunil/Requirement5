package com.mycart.route;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
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
        onException(Exception.class)
                .handled(true)
                .log(LoggingLevel.ERROR, "Route failed: ${exception.message}, itemId: ${exchangeProperty.itemId}, currentTs: ${exchangeProperty.currentTs}")
                .stop();

        JaxbDataFormat trendXmlFormat = new JaxbDataFormat(TrendXml.class.getPackage().getName());
        JaxbDataFormat reviewXmlFormat = new JaxbDataFormat(ReviewXml.class.getPackage().getName());
        JacksonDataFormat jsonFormat = new JacksonDataFormat(StoreJson.class);

        String mongoUri = "mongodb:mongoDbComponent?database={{app.mongodb.database}}";

        from("quartz://fileExport?cron={{app.scheduler.cron}}&stateful=true")
                .routeId("fileExport")
                .bean("itemProcessor", "setCurrentTimestamp")
                .to("direct:fetchControlRef")
                .to("direct:processItems")
                .choice()
                .when(simple("${exchangeProperty.itemsProcessed} == true"))
                .to("direct:updateControlRef")
                .log(LoggingLevel.INFO, "Proceeding to update controlRef")
                .end()
                .log(LoggingLevel.INFO, "File export completed, itemsProcessed: ${exchangeProperty.itemsProcessed}");

        from("direct:fetchControlRef")
                .routeId("fetchControlRef")
                .setBody(constant(new Document("_id", "global")))
                .setHeader("MongoInputBody", body())
                .setHeader("MongoOperationUri", constant(mongoUri + "&collection={{app.control.collection}}&operation=findOneByQuery&outputType=Document"))
                .setProperty("MongoRetryCount", constant(1))
                .to("direct:retryMongoOperation")
                .bean("controlRefProcessor", "fetchControlRefs");

        from("direct:processItems")
                .routeId("processItems")
                .doTry()
                .bean("itemProcessor", "prepareItemQuery")
                .setHeader(MongoDbConstants.LIMIT, constant(Integer.parseInt(getContext().resolvePropertyPlaceholders("{{app.records.processLimit}}"))))
                .setHeader("MongoInputBody", body())
                .setHeader("MongoOperationUri", constant(mongoUri + "&collection={{app.item.collection}}&operation=findAll"))
                .setProperty("MongoRetryCount", constant(1))
                .to("direct:retryMongoOperation")
                .bean("itemProcessor", "validateItemList")
                .bean("itemProcessor", "filterValidItems")
                .bean("itemProcessor", "logFetchedItems")
                .choice()
                .when(simple("${body} != null && ${body.size()} > 0"))
                .bean("itemProcessor", "storeOriginalExchange")
                .setProperty("itemsProcessed", constant(true))
                .split(body())
                .bean("itemProcessor", "enrichWithCategory")
                .setHeader("MongoInputBody", body())
                .setHeader("MongoOperationUri", constant(mongoUri + "&collection={{app.category.collection}}&operation=findOneByQuery&outputType=Document"))
                .setProperty("MongoRetryCount", constant(1))
                .to("direct:retryMongoOperation")
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
                .log(LoggingLevel.WARN, "Failed to write trend XML for item ${exchangeProperty.itemId}: ${exception.message}")
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
                .log(LoggingLevel.WARN, "Failed to write review XML for item ${exchangeProperty.itemId}: ${exception.message}")
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
                .log(LoggingLevel.WARN, "Failed to write store JSON for item ${exchangeProperty.itemId}: ${exception.message}")
                .end()
                .end()
                .bean("itemProcessor", "restoreOriginalExchange")
                .endChoice()
                .otherwise()
                .setProperty("itemsProcessed", constant(false))
                .log(LoggingLevel.INFO, "No valid items to process, skipping split")
                .end()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed processing items: ${exception.message}, currentTs: ${exchangeProperty.currentTs}")
                .setProperty("itemsProcessed", constant(false))
                .end();

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


        from("direct:updateControlRef")
                .routeId("updateControlRef")
                .bean("controlRefProcessor", "updateControlRef")
                .setHeader("MongoInputBody", body())
                .setHeader("MongoOperationUri", constant(mongoUri + "&collection={{app.control.collection}}&operation=save"))
                .setProperty("MongoRetryCount", constant(1))
                .to("direct:retryMongoOperation")
                .log(LoggingLevel.INFO, "controlRef updated with lastProcessTs: ${exchangeProperty.currentTs}");

        from("direct:retryMongoOperation")
                .routeId("retryMongoOperation")
                .doTry()
                .setBody(header("MongoInputBody"))
                .toD("${header.MongoOperationUri}")
                .doCatch(Exception.class)
                .log(LoggingLevel.WARN, "MongoDB retry ${exchangeProperty.MongoRetryCount} failed: ${exception.message}")
                .choice()
                .when(simple("${exchangeProperty.MongoRetryCount} <= {{app.retry.max}}"))
                .delay().method("mongoRetryHelper", "getBackoffDelay")
                .log(LoggingLevel.INFO, "Retrying MongoDB operation attempt #${exchangeProperty.MongoRetryCount} after backoff")
                .to("direct:retryMongoOperation")
                .endChoice()
                .otherwise()
                .log(LoggingLevel.ERROR, "All retry attempts failed for MongoDB operation")
                .throwException(Exception.class, "MongoDB operation failed after retries")
                .endChoice()
                .end();
    }
}

