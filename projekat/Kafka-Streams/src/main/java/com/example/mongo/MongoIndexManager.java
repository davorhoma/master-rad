package com.example.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.HashSet;
import java.util.Set;

public final class MongoIndexManager {

	private MongoIndexManager() {
	}

	public static void ensureIndexes(
			MongoDatabase database,
			String collectionName) {

		if (database == null) {
			throw new IllegalArgumentException("MongoDatabase cannot be null");
		}

		if (collectionName == null || collectionName.isBlank()) {
			throw new IllegalArgumentException("Mongo collection name cannot be null or blank");
		}

		switch (collectionName) {

			case "query_2_views_by_theme_timeline":
				ensureIndex(
						database,
						collectionName,
						Indexes.ascending(
								"platform",
								"theme",
								"time_window"),
						"platform_1_theme_1_time_window_1");
				break;

			case "query_3_popular_gaming_keywords":
				ensureIndex(
						database,
						collectionName,
						Indexes.ascending(
								"platform",
								"keyword"),
						"platform_1_keyword_1");
				break;

			case "query_4_peak_popularity_days":
				ensureIndex(
						database,
						collectionName,
						Indexes.ascending(
								"platform",
								"video_id"),
						"platform_1_video_id_1");

				ensureIndex(
						database,
						collectionName,
						Indexes.ascending("theme"),
						"theme_1");

				ensureIndex(
						database,
						collectionName,
						Indexes.ascending("days_to_peak"),
						"days_to_peak_1");
				break;

			case "query_10_time_staying_in_top_rankings":
				ensureIndex(
						database,
						collectionName,
						Indexes.ascending("video_id"),
						"video_id_1");

				ensureIndex(
						database,
						collectionName,
						Indexes.ascending("platform"),
						"platform_1");

				ensureIndex(
						database,
						collectionName,
						Indexes.ascending("days_in_top"),
						"days_in_top_1");
				break;

			default:
				System.out.println(
						"No indexes configured for collection: "
								+ collectionName);
		}
	}

	private static void ensureIndex(
			MongoDatabase database,
			String collectionName,
			Bson indexKeys,
			String indexName) {

		MongoCollection<Document> collection = database.getCollection(collectionName);

		Set<String> existingIndexNames = new HashSet<>();

		collection.listIndexes().forEach(index -> {
			String name = index.getString("name");

			if (name != null) {
				existingIndexNames.add(name);
			}
		});

		if (existingIndexNames.contains(indexName)) {
			System.out.println(
					"MongoDB index already exists: "
							+ collectionName
							+ " -> "
							+ indexName);
			return;
		}

		collection.createIndex(
				indexKeys,
				new IndexOptions().name(indexName));

		System.out.println(
				"MongoDB index created: "
						+ collectionName
						+ " -> "
						+ indexName);
	}
}