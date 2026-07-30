import { ContextAccessor } from "@omnixys/context-ts";
import {
  KAFKA_HANDLER,
  KAFKA_HEADERS,
  KAFKA_EVENT_PUBLISHER,
  KafkaCircuitBreakerService,
  KafkaCircuitOpenError,
  KafkaConsumerService,
  KafkaHandler,
  KafkaIdempotencyService,
  KafkaLifecycleService,
  KafkaModule,
  KafkaProducerService,
  KafkaRetryService,
  KafkaTopics,
  getAllKafkaTopics,
  getKafkaTopicCatalog,
  getKafkaTopicReconciliationCatalog,
  inferKafkaTopicPolicy,
  validateKafkaTopicCatalog,
} from "../dist/index.js";
import assert from "node:assert/strict";
import test from "node:test";

test("producer propagates canonical context and emits a stable envelope", async () => {
  const transport = createProducerTransport();
  const producer = new KafkaProducerService(transport, {
    clientId: "users",
    brokers: ["localhost:9092"],
    groupId: "users",
    serviceName: "users-service",
  });

  await producer.onModuleInit();

  await ContextAccessor.run(
    {
      requestId: "request-1",
      correlationId: "correlation-1",
      actorId: "actor-1",
      userId: "user-1",
      tenantId: "tenant-1",
    },
    () =>
      producer.send({
        topic: KafkaTopics.user.deleteUser,
        payload: { userId: "user-2" },
        eventId: "event-1",
        headers: { [KAFKA_HEADERS.REQUEST_ID]: "must-not-override" },
      }),
  );

  const sent = transport.sent[0];
  const message = sent.messages[0];
  const envelope = JSON.parse(message.value);
  assert.equal(envelope.eventId, "event-1");
  assert.equal(envelope.eventName, KafkaTopics.user.deleteUser);
  assert.equal(envelope.service, "users-service");
  assert.equal(
    message.headers[KAFKA_HEADERS.REQUEST_ID].toString(),
    "request-1",
  );
  assert.equal(
    message.headers[KAFKA_HEADERS.CORRELATION_ID].toString(),
    "correlation-1",
  );
  assert.equal(message.headers[KAFKA_HEADERS.ACTOR_ID].toString(), "actor-1");
  assert.equal(message.headers[KAFKA_HEADERS.TENANT_ID].toString(), "tenant-1");
});

test("event media uploads have a stable canonical topic", () => {
  assert.equal(KafkaTopics.event.mediaUploaded, "event.media.uploaded");
  assert.equal(
    KafkaTopics.event.milestoneRecorded,
    "event.milestone.recorded",
  );
});

test("topic catalog is deterministic, unique, and policy-aware", () => {
  const catalog = getKafkaTopicCatalog();
  const validation = validateKafkaTopicCatalog(catalog);
  const topics = catalog.topics.map((entry) => entry.topic);

  assert.equal(validation.valid, true, validation.errors.join("\n"));
  assert.deepEqual(validation.warnings, []);
  assert.deepEqual(topics, [...topics].sort((left, right) => left.localeCompare(right)));
  assert.equal(new Set(topics).size, topics.length);
  assert.deepEqual(new Set(getAllKafkaTopics()), new Set(topics));

  const createUser = catalog.topics.find(
    (entry) => entry.topic === KafkaTopics.user.createUser,
  );
  assert.deepEqual(createUser?.producers, ["authentication"]);
  assert.deepEqual(createUser?.consumers, ["user"]);
  assert.equal(createUser?.owner, "user");

  const retry = catalog.topics.find(
    (entry) => entry.topic === KafkaTopics.whatsapp.retry,
  );
  assert.equal(retry?.policy, "retry");

  const dlq = catalog.topics.find(
    (entry) => entry.topic === KafkaTopics.whatsapp.dlq,
  );
  assert.equal(dlq?.policy, "dlq");
  assert.equal(dlq?.config["retention.ms"], "2592000000");

  const logstream = catalog.topics.find(
    (entry) => entry.topic === KafkaTopics.logstream.log,
  );
  assert.equal(logstream?.policy, "logstream");
  assert.equal(logstream?.partitions, 3);
});

test("topic reconciliation catalog includes runtime retry and DLQ topics without nested failure topics", () => {
  const baseCatalog = getKafkaTopicCatalog();
  const catalog = getKafkaTopicReconciliationCatalog();
  const validation = validateKafkaTopicCatalog(catalog);
  const topics = catalog.topics.map((entry) => entry.topic);

  assert.equal(validation.valid, true, validation.errors.join("\n"));
  assert.equal(new Set(topics).size, topics.length);

  for (const entry of baseCatalog.topics) {
    assert.equal(topics.includes(entry.topic), true, entry.topic);
  }

  const base = KafkaTopics.user.createUser;
  const retry = catalog.topics.find((entry) => entry.topic === `${base}.retry`);
  const dlq = catalog.topics.find((entry) => entry.topic === `${base}.dlq`);

  assert.equal(retry?.policy, "retry");
  assert.equal(retry?.config["retention.ms"], "86400000");
  assert.equal(dlq?.policy, "dlq");
  assert.equal(dlq?.config["retention.ms"], "2592000000");
  assert.equal(topics.includes(`${KafkaTopics.whatsapp.retry}.retry`), false);
  assert.equal(topics.includes(`${KafkaTopics.whatsapp.retry}.dlq`), false);
  assert.equal(topics.includes(`${KafkaTopics.whatsapp.dlq}.retry`), false);
  assert.equal(catalog.topics.length > baseCatalog.topics.length, true);
});

test("topic policy inference handles retry, DLQ, compacted, and logstream topics", () => {
  assert.equal(inferKafkaTopicPolicy("notification.retry.whatsapp"), "retry");
  assert.equal(inferKafkaTopicPolicy("notification.dlq.whatsapp"), "dlq");
  assert.equal(inferKafkaTopicPolicy("logstream.log"), "logstream");
  assert.equal(inferKafkaTopicPolicy("user.profile.compacted"), "compacted");
  assert.equal(inferKafkaTopicPolicy("authentication.create.user"), "default");
});

test("producer batches by topic and exposes graceful lifecycle APIs", async () => {
  const transport = createProducerTransport();
  const producer = new KafkaProducerService(transport);
  await producer.onModuleInit();
  await producer.send(
    KafkaTopics.user.deleteUser,
    { userId: "legacy-user" },
    "legacy-service",
  );
  assert.equal(
    JSON.parse(transport.sent[0].messages[0].value).service,
    "legacy-service",
  );
  await producer.sendBatch([
    { topic: KafkaTopics.user.deleteUser, payload: { userId: "user-1" } },
    { topic: KafkaTopics.user.deleteUser, payload: { userId: "user-2" } },
    {
      topic: KafkaTopics.ticket.deleteUserTickets,
      payload: { userId: "user-1" },
    },
  ]);

  assert.equal(transport.batches[0].topicMessages.length, 2);
  assert.equal(transport.batches[0].topicMessages[0].messages.length, 2);
  assert.equal(producer.health().healthy, true);
  assert.equal(producer.diagnostics().activeSends, 0);
  await producer.drain();
  await producer.close();
  await producer.close();
  assert.equal(transport.disconnectCalls, 1);
  assert.equal(producer.status(), "stopped");
});

test("retry policy preserves headers, avoids nested retry topics, and routes to DLQ", async () => {
  const calls = [];
  const producer = {
    async rawSendWithHeaders(topic, value, headers) {
      calls.push({ topic, value, headers });
    },
  };
  const retry = new KafkaRetryService(producer, {
    clientId: "test",
    brokers: [],
    groupId: "test",
    serviceName: "test",
    retry: { maxRetries: 2, initialDelayMs: 10, maxDelayMs: 100 },
  });

  await retry.handleRetry(
    "orders.retry",
    '{"event":true}',
    {
      "x-original-topic": "orders",
      "x-retry-count": "1",
      "x-request-id": "request-1",
    },
    new Error("temporary"),
  );
  assert.equal(calls[0].topic, "orders.retry");
  assert.equal(calls[0].headers["x-retry-count"], "2");
  assert.equal(calls[0].headers["x-request-id"], "request-1");
  assert.equal(calls[0].headers["x-original-topic"], "orders");

  await retry.handleRetry(
    "orders.retry",
    "{}",
    calls[0].headers,
    new Error("final"),
  );
  assert.equal(calls[1].topic, "orders.dlq");
  assert.equal(calls[1].headers["x-retry-count"], "2");
  assert.equal(calls[1].headers["x-error"], "final");
  assert.equal(
    retry.retryTopic(KafkaTopics.whatsapp.retry),
    KafkaTopics.whatsapp.retry,
  );
  assert.equal(
    retry.deadLetterTopic(KafkaTopics.whatsapp.retry),
    KafkaTopics.whatsapp.dlq,
  );
  assert.deepEqual(retry.retryTopics([
    KafkaTopics.whatsapp.outgoing,
    KafkaTopics.whatsapp.retry,
    KafkaTopics.whatsapp.dlq,
  ]), [`${KafkaTopics.whatsapp.outgoing}.retry`]);
});

test("consumer subscribes to retry topics and restores canonical context for batches", async () => {
  const transport = createConsumerTransport();
  const observed = [];
  const dispatcher = {
    ready: Promise.resolve(),
    getRegisteredTopics: () => [KafkaTopics.user.deleteUser],
    async dispatch(topic, payload, eventContext) {
      observed.push({
        topic,
        payload,
        eventContext,
        context: ContextAccessor.get(),
      });
    },
  };
  const idempotency = createIdempotency();
  const retry = createRetryPolicy();
  const circuit = createCircuit();
  const consumer = new KafkaConsumerService(
    transport,
    dispatcher,
    idempotency,
    retry,
    circuit,
    { fromBeginning: true },
  );

  await consumer.start();
  assert.deepEqual(transport.subscription.topics, [
    KafkaTopics.user.deleteUser,
    `${KafkaTopics.user.deleteUser}.retry`,
  ]);
  assert.equal(transport.subscription.fromBeginning, true);

  const envelope = {
    eventId: "event-1",
    eventName: KafkaTopics.user.deleteUser,
    eventVersion: "1",
    service: "authentication",
    timestamp: new Date().toISOString(),
    payload: { userId: "user-1" },
  };
  const result = await runBatch(transport.eachBatch, {
    topic: KafkaTopics.user.deleteUser,
    messages: [
      createMessage(envelope, {
        [KAFKA_HEADERS.REQUEST_ID]: "request-2",
        [KAFKA_HEADERS.CORRELATION_ID]: "correlation-2",
        [KAFKA_HEADERS.ACTOR_ID]: "actor-2",
        [KAFKA_HEADERS.TENANT_ID]: "tenant-2",
      }),
    ],
  });

  assert.deepEqual(result.offsets, ["0"]);
  assert.equal(result.commits, 1);
  assert.equal(observed[0].topic, KafkaTopics.user.deleteUser);
  assert.equal(observed[0].context.requestId, "request-2");
  assert.equal(observed[0].context.correlationId, "correlation-2");
  assert.equal(observed[0].context.principal.actorId, "actor-2");
  assert.equal(observed[0].context.tenant.tenantId, "tenant-2");
  assert.equal(observed[0].eventContext.requestId, "request-2");
  assert.deepEqual(idempotency.processed, ["event-1"]);
  assert.equal(consumer.health().healthy, true);
  assert.equal(consumer.diagnostics().inFlight, 0);

  await consumer.close();
  assert.equal(transport.stopCalls, 1);
  assert.equal(transport.disconnectCalls, 1);
});

test("consumer resolves an offset only after failed messages reach retry policy", async () => {
  const transport = createConsumerTransport();
  const retries = [];
  const consumer = new KafkaConsumerService(
    transport,
    {
      ready: Promise.resolve(),
      getRegisteredTopics: () => ["orders"],
      async dispatch() {
        throw new Error("handler failure");
      },
    },
    createIdempotency(),
    {
      retryTopics: (topics) => topics.map((topic) => `${topic}.retry`),
      originalTopic: (topic, headers) =>
        headers["x-original-topic"] ?? topic.replace(/\.retry$/, ""),
      diagnostics: () => ({}),
      async handleRetry(topic, raw, headers, error) {
        retries.push({ topic, raw, headers, error });
      },
    },
    createCircuit(),
  );
  await consumer.start();

  const envelope = {
    eventId: "event-2",
    eventName: "orders",
    eventVersion: "1",
    service: "orders",
    timestamp: new Date().toISOString(),
    payload: { orderId: "order-1" },
  };
  const result = await runBatch(transport.eachBatch, {
    topic: "orders.retry",
    messages: [
      createMessage(envelope, {
        "x-original-topic": "orders",
        "x-retry-count": "1",
        [KAFKA_HEADERS.REQUEST_ID]: "request-3",
      }),
    ],
  });

  assert.equal(retries.length, 1);
  assert.equal(retries[0].topic, "orders.retry");
  assert.equal(retries[0].headers["x-original-topic"], "orders");
  assert.deepEqual(result.offsets, ["0"]);
  await consumer.close();
});

test("per-topic circuit breaker opens without blocking unrelated topics", async () => {
  const breaker = new KafkaCircuitBreakerService();
  await ContextAccessor.run(
    { requestId: "request-4", correlationId: "correlation-4" },
    async () => {
      for (let index = 0; index < 5; index += 1) {
        await assert.rejects(
          breaker.execute(async () => {
            throw new Error("failure");
          }, "orders"),
        );
      }
      assert.equal(breaker.status("orders"), "OPEN");
      await assert.rejects(
        breaker.execute(async () => true, "orders"),
        (error) => {
          assert.ok(error instanceof KafkaCircuitOpenError);
          assert.equal(error.requestId, "request-4");
          assert.equal(error.correlationId, "correlation-4");
          return true;
        },
      );
      assert.equal(await breaker.execute(async () => "ok", "payments"), "ok");
    },
  );
});

test("idempotency fallback remains operational without a cache provider", async () => {
  const idempotency = new KafkaIdempotencyService(undefined, {
    idempotency: { ttlSeconds: 60 },
  });
  assert.equal(await idempotency.isProcessed("event-1"), false);
  await idempotency.markProcessed("event-1");
  assert.equal(await idempotency.isProcessed("event-1"), true);
  assert.equal(idempotency.diagnostics().storage, "memory");
});

test("module async registration and compatibility decorator remain available", () => {
  const module = KafkaModule.forRootAsync({
    useFactory: () => ({
      clientId: "test",
      brokers: ["localhost:9092"],
      groupId: "test",
      serviceName: "test",
    }),
  });
  assert.ok(module.providers.length > 0);
  assert.ok(
    module.providers.some(
      (provider) =>
        provider.provide === KAFKA_EVENT_PUBLISHER &&
        provider.useExisting === KafkaProducerService,
    ),
  );
  assert.ok(module.exports.includes(KAFKA_EVENT_PUBLISHER));

  class Handler {}
  KafkaHandler("handler-name")(Handler);
  assert.equal(Reflect.getMetadata(KAFKA_HANDLER, Handler), "handler-name");
});

test("aggregate lifecycle delegates health, drain, and close", async () => {
  const calls = [];
  const producer = {
    status: () => "ready",
    health: () => ({ healthy: true, status: "ready" }),
    diagnostics: () => ({}),
    async drain() {
      calls.push("producer-drain");
    },
    async close() {
      calls.push("producer-close");
    },
  };
  const consumer = {
    ready: () => true,
    health: () => ({ healthy: true, status: "running" }),
    diagnostics: () => ({}),
    async drain() {
      calls.push("consumer-drain");
    },
    async close() {
      calls.push("consumer-close");
    },
  };
  const lifecycle = new KafkaLifecycleService(producer, consumer);
  assert.equal(lifecycle.ready(), true);
  assert.equal(lifecycle.health().healthy, true);
  await lifecycle.drain();
  await lifecycle.close();
  assert.deepEqual(calls.slice(0, 2).sort(), [
    "consumer-drain",
    "producer-drain",
  ]);
  assert.deepEqual(calls.slice(2), ["consumer-close", "producer-close"]);
});

function createProducerTransport() {
  return {
    sent: [],
    batches: [],
    disconnectCalls: 0,
    async send(value) {
      this.sent.push(value);
    },
    async sendBatch(value) {
      this.batches.push(value);
    },
    async connect() {},
    async disconnect() {
      this.disconnectCalls += 1;
    },
  };
}

function createConsumerTransport() {
  return {
    subscription: undefined,
    eachBatch: undefined,
    stopCalls: 0,
    disconnectCalls: 0,
    async subscribe(value) {
      this.subscription = value;
    },
    async run({ eachBatch }) {
      this.eachBatch = eachBatch;
    },
    async stop() {
      this.stopCalls += 1;
    },
    async disconnect() {
      this.disconnectCalls += 1;
    },
  };
}

function createIdempotency() {
  return {
    processed: [],
    async isProcessed() {
      return false;
    },
    async markProcessed(id) {
      this.processed.push(id);
    },
    diagnostics() {
      return {};
    },
  };
}

function createRetryPolicy() {
  return {
    retryTopics: (topics) => topics.map((topic) => `${topic}.retry`),
    originalTopic: (topic, headers) =>
      headers["x-original-topic"] ?? topic.replace(/\.retry$/, ""),
    async handleRetry() {},
    diagnostics: () => ({}),
  };
}

function createCircuit() {
  return {
    async execute(operation) {
      return operation();
    },
    diagnostics() {
      return {};
    },
  };
}

function createMessage(envelope, headers = {}) {
  return {
    key: null,
    value: Buffer.from(JSON.stringify(envelope)),
    timestamp: String(Date.now()),
    attributes: 0,
    offset: "0",
    headers: Object.fromEntries(
      Object.entries(headers).map(([key, value]) => [key, Buffer.from(value)]),
    ),
  };
}

async function runBatch(eachBatch, { topic, messages }) {
  const offsets = [];
  let commits = 0;
  await eachBatch({
    batch: { topic, partition: 0, highWatermark: "1", messages },
    resolveOffset: (offset) => offsets.push(offset),
    heartbeat: async () => {},
    pause: () => () => {},
    commitOffsetsIfNecessary: async () => {
      commits += 1;
    },
    uncommittedOffsets: () => ({}),
    isRunning: () => true,
    isStale: () => false,
  });
  return { offsets, commits };
}
