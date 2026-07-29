import assert from "node:assert/strict";
import test from "node:test";
import { getKafkaTopicCatalog, KafkaTopics } from "../dist/index.js";

test("domain fact topics are explicitly classified and versioned", () => {
  const catalog = getKafkaTopicCatalog();
  const rejected = catalog.topics.find(
    (entry) => entry.topic === KafkaTopics.ticket.scanRejectedFact,
  );
  assert.equal(rejected?.classification, "FACT");
  assert.equal(rejected?.version, 1);
  assert.deepEqual(rejected?.producers, ["ticket"]);
  assert.deepEqual(rejected?.consumers, ["analytics"]);
});

test("commands are never inferred as analytics facts", () => {
  const catalog = getKafkaTopicCatalog();
  const command = catalog.topics.find(
    (entry) => entry.topic === KafkaTopics.ticket.create,
  );
  assert.equal(command?.classification, "COMMAND");
});

test("event lifecycle facts remain separate from legacy event topics", () => {
  const catalog = getKafkaTopicCatalog();
  assert.equal(KafkaTopics.event.created, "event.created");
  assert.equal(KafkaTopics.event.createdFact, "event.created.v1");
  for (const topic of [
    KafkaTopics.event.createdFact,
    KafkaTopics.event.updatedFact,
    KafkaTopics.event.deletedFact,
  ]) {
    const fact = catalog.topics.find((entry) => entry.topic === topic);
    assert.equal(fact?.classification, "FACT");
    assert.deepEqual(fact?.producers, ["event"]);
    assert.deepEqual(fact?.consumers, ["analytics"]);
  }
});
