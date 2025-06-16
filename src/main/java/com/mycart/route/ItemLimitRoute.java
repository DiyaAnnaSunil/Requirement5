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
        // Global exception handling at the very top
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
                .to("direct:updateControlRef")
                .log(LoggingLevel.INFO, "File export completed");

        from("direct:fetchControlRef")
                .routeId("fetchControlRef")
                .log(LoggingLevel.DEBUG, "Before fetching controlRef")
                .setBody(constant(new Document("_id", "global")))
                .log(LoggingLevel.DEBUG, "Set query for controlRef: ${body}")
                .to(mongoUri + "&collection={{app.control.collection}}&operation=findOneByQuery&outputType=Document")
                .bean("controlRefProcessor", "fetchControlRefs")
                .log(LoggingLevel.INFO, "Fetched controlRefMap with ${exchangeProperty.controlRefMap.size()} entries");

        from("direct:processItems")
                .routeId("processItems")
                .doTry()
                .bean("itemProcessor", "prepareItemQuery")
                .log(LoggingLevel.DEBUG, "After prepareItemQuery, query: ${body}")
                .setHeader(MongoDbConstants.LIMIT, constant(Integer.parseInt(getContext().resolvePropertyPlaceholders("{{app.records.processLimit}}"))))
                .to(mongoUri + "&collection={{app.item.collection}}&operation=findAll")
                .bean("itemProcessor", "validateItemList")
                .bean("itemProcessor", "filterValidItems")
                .bean("itemProcessor", "logFetchedItems")
                .bean("itemProcessor", "storeOriginalExchange")
                .split(body())
                .log(LoggingLevel.DEBUG, "Processing item ${exchangeProperty.itemId}")
                .bean("itemProcessor", "enrichWithCategory")
                .log(LoggingLevel.DEBUG, "Executing category query for item ${exchangeProperty.itemId}, query: ${body}")
                .to(mongoUri + "&collection={{app.category.collection}}&operation=findOneByQuery&outputType=Document")
                .bean("itemProcessor", "validateCategoryResult")
                .bean("itemProcessor", "processCategoryQuery")
                .bean("itemProcessor", "mapItemData")
                .doTry()
                .bean("itemProcessor", "prepareTrendXml")
                .choice()
                .when(body().isNotNull())
                .marshal(trendXmlFormat)
                .setHeader("OutputFolder", constant("trend"))
                .to("direct:writeToFile")
                .otherwise()
                .log(LoggingLevel.WARN, "Skipping null trend XML for item ${exchangeProperty.itemId}")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to write trend XML for item ${exchangeProperty.itemId}: ${exception.message}")
                .end()
                .doTry()
                .bean("itemProcessor", "prepareReviewXml")
                .choice()
                .when(body().isNotNull())
                .marshal(reviewXmlFormat)
                .setHeader("OutputFolder", constant("review"))
                .to("direct:writeToFile")
                .otherwise()
                .log(LoggingLevel.WARN, "Skipping null review XML for item ${exchangeProperty.itemId}")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to write review XML for item ${exchangeProperty.itemId}: ${exception.message}")
                .end()
                .doTry()
                .bean("itemProcessor", "prepareStoreJson")
                .choice()
                .when(body().isNotNull())
                .marshal(jsonFormat)
                .setHeader("OutputFolder", constant("store"))
                .to("direct:writeToFile")
                .otherwise()
                .log(LoggingLevel.WARN, "Skipping null store JSON for item ${exchangeProperty.itemId}")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to write store JSON for item ${exchangeProperty.itemId}: ${exception.message}")
                .end()
                .log(LoggingLevel.DEBUG, "Completed file writes for item ${exchangeProperty.itemId}")
                .end()
                .log(LoggingLevel.DEBUG, "Completed processItems split, currentTs: ${exchangeProperty.currentTs}")
                .bean("itemProcessor", "restoreOriginalExchange")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed processing items: ${exception.message}, currentTs: ${exchangeProperty.currentTs}")
                .end();

        from("direct:writeToFile")
                .routeId("writeToFile")
                .doTry()
                .choice()
                .when(simple("${header.OutputFolder} == 'trend'"))
                .to("file://{{app.output.item-trend-analyzer}}?fileName=${header.CamelFileName}&fileExist=Ignore")
                .when(simple("${header.OutputFolder} == 'review'"))
                .to("file://{{app.output.item-review-aggregator}}?fileName=${header.CamelFileName}&fileExist=Ignore")
                .when(simple("${header.OutputFolder} == 'store'"))
                .to("file://{{app.output.storefront-app}}?fileName=${header.CamelFileName}&fileExist=Ignore")
                .otherwise()
                .log(LoggingLevel.ERROR, "Unknown OutputFolder: ${header.CamelFileName}")
                .stop()
                .end()
                .log(LoggingLevel.INFO, "Saved file: ${header.CamelFileName}")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to write file: ${header.CamelFileName}, error: ${exception.message}")
                .end();

        from("direct:writeTrendXml")
                .routeId("writeTrendXml")
                .bean("itemProcessor", "prepareTrendXml")
                .choice()
                .when(body().isNotNull())
                .doTry()
                .marshal(trendXmlFormat)
                .setHeader("OutputFolder", constant("trend"))
                .to("direct:writeToFile")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to write trend XML: ${header.CamelFileName}, error: ${exception.message}")
                .end()
                .endChoice()
                .when(body().isNull())
                .log(LoggingLevel.WARN, "Skipping null trend XML")
                .endChoice();

        from("direct:writeReviewXml")
                .routeId("writeReviewXml")
                .bean("itemProcessor", "prepareReviewXml")
                .choice()
                .when(body().isNotNull())
                .doTry()
                .marshal(reviewXmlFormat)
                .setHeader("OutputFolder", constant("review"))
                .to("direct:writeToFile")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to write review XML: ${header.CamelFileName}, error: ${exception.message}")
                .end()
                .endChoice()
                .when(body().isNull())
                .log(LoggingLevel.WARN, "Skipping null review XML")
                .endChoice();

        from("direct:writeStoreJson")
                .routeId("writeStoreJson")
                .bean("itemProcessor", "prepareStoreJson")
                .choice()
                .when(body().isNotNull())
                .doTry()
                .marshal(jsonFormat)
                .setHeader("OutputFolder", constant("store"))
                .to("direct:writeToFile")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to write store JSON: ${header.CamelFileName}, error: ${exception.message}")
                .end()
                .endChoice()
                .when(body().isNull())
                .log(LoggingLevel.WARN, "Skipping null store JSON")
                .endChoice();

        from("direct:updateControlRef")
                .routeId("updateControlRef")
                .log(LoggingLevel.DEBUG, "Starting controlRef update with currentTs: ${exchangeProperty.currentTs}")
                .bean("controlRefProcessor", "updateControlRef")
                .choice()
                .when(body().isNotNull())
                .doTry()
                .to(mongoUri + "&collection={{app.control.collection}}&operation=save")
                .log(LoggingLevel.INFO, "controlRef updated with lastProcessTs: ${exchangeProperty.currentTs}")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to save controlRef to MongoDB: ${exception.message}")
                .end()
                .endChoice()
                .when(body().isNull())
                .log(LoggingLevel.ERROR, "Skipped controlRef update due to null body, currentTs: ${exchangeProperty.currentTs}")
                .endChoice();
    }
}