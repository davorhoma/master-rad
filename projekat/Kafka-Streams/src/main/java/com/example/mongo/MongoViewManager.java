package com.example.mongo;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;

public final class MongoViewManager {

    private MongoViewManager() {
    }

    public static void ensureViews(MongoDatabase database, String collectionName) {

        if (database == null) {
            throw new IllegalArgumentException("MongoDatabase cannot be null");
        }

        ensureQuery5View(database, collectionName);
    }

    private static void ensureQuery5View(MongoDatabase database, String sourceCollection) {

        String viewName = "query_5_platform_overlap_top20";

        boolean exists = database.listCollectionNames()
                .into(new java.util.ArrayList<>())
                .contains(viewName);

        if (exists) {
            System.out.println(
                    "MongoDB view already exists: " + viewName);
            return;
        }

        List<Document> pipeline = Arrays.asList(
                new Document("$group",
                        new Document("_id",
                                new Document("theme", "$theme")
                                        .append("platform", "$platform"))
                                .append("appeared_times",
                                        new Document("$sum", "$appeared_times"))),

                new Document("$project",
                        new Document("_id", 0)
                                .append("theme", "$_id.theme")
                                .append("platform", "$_id.platform")
                                .append("appeared_times", 1)),

                new Document("$group",
                        new Document("_id", "$theme")
                                .append("platforms",
                                        new Document("$push",
                                                new Document("platform", "$platform")
                                                        .append("appeared_times", "$appeared_times")))
                                .append("total_appeared_times",
                                        new Document("$sum", "$appeared_times"))),

                new Document("$sort",
                        new Document("total_appeared_times", -1)),

                new Document("$limit", 20),

                new Document("$project",
                        new Document("_id", 0)
                                .append("theme", "$_id")
                                .append("platforms",
                                        Arrays.asList("YouTube", "TikTok"))
                                .append("existing_platforms", "$platforms")),

                new Document("$unwind", "$platforms"),

                new Document("$set",
                        new Document("platform", "$platforms")),

                new Document("$set",
                        new Document("matched",
                                new Document("$arrayElemAt",
                                        Arrays.asList(
                                                new Document("$filter",
                                                        new Document("input", "$existing_platforms")
                                                                .append("as", "p")
                                                                .append("cond",
                                                                        new Document("$eq",
                                                                                Arrays.asList(
                                                                                        "$$p.platform",
                                                                                        "$platform")))),
                                                0)))),

                new Document("$project",
                        new Document("_id", 0)
                                .append("theme", 1)
                                .append("platform", 1)
                                .append("appeared_times",
                                        new Document("$ifNull",
                                                Arrays.asList(
                                                        "$matched.appeared_times",
                                                        0)))));

        database.createView(
                viewName,
                sourceCollection,
                pipeline);

        System.out.println(
                "MongoDB view created: " + viewName);
    }
}