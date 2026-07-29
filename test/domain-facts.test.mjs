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
