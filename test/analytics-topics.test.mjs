import assert from "node:assert/strict";
import test from "node:test";
import {
  getKafkaTopicCatalog,
  KafkaTopics,
  validateKafkaTopicCatalog,
} from "../dist/types/index.js";

test("registers the analytics platform topic family", () => {
  assert.equal(
    KafkaTopics.analytics.eventsIngested,
    "analytics.events.ingested",
  );
  assert.equal(
    KafkaTopics.analytics.dataQualityDetected,
    "analytics.data-quality.detected",
  );
});

test("assigns scalable ingestion metadata", () => {
  const catalog = getKafkaTopicCatalog();
  const ingestion = catalog.topics.find(
    ({ topic }) => topic === KafkaTopics.analytics.eventsIngested,
  );
  assert.equal(ingestion?.owner, "analytics");
  assert.equal(ingestion?.partitions, 12);
  assert.equal(validateKafkaTopicCatalog(catalog).valid, true);
});
