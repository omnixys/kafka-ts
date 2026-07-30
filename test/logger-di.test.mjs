import assert from "node:assert/strict";
import test from "node:test";
import "reflect-metadata";

import { Global, Module } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { LoggerModule, OmnixysLogger } from "@omnixys/logger-ts";
import {
  KAFKA_EVENT_PUBLISHER,
  KAFKA_OPTIONS,
  KAFKA_PRODUCER,
  KafkaProducerService,
} from "../dist/index.js";

test("Kafka-backed logger and producer compile without a dependency cycle", async () => {
  class KafkaPublisherTestModule {}
  Global()(KafkaPublisherTestModule);
  Module({
    providers: [
      {
        provide: KAFKA_OPTIONS,
        useValue: {
          clientId: "kafka-di-test",
          brokers: ["localhost:9092"],
          groupId: "kafka-di-test",
          serviceName: "kafka-di-test",
        },
      },
      {
        provide: KAFKA_PRODUCER,
        useValue: {
          send: async () => undefined,
          sendBatch: async () => undefined,
          connect: async () => undefined,
          disconnect: async () => undefined,
        },
      },
      KafkaProducerService,
      { provide: KAFKA_EVENT_PUBLISHER, useExisting: KafkaProducerService },
    ],
    exports: [KafkaProducerService, KAFKA_EVENT_PUBLISHER],
  })(KafkaPublisherTestModule);

  class KafkaLoggerTestModule {}
  Module({
    imports: [
      KafkaPublisherTestModule,
      LoggerModule.forRoot({
        serviceName: "kafka-di-test",
        kafka: { enabled: true, topic: "logstream.input" },
      }),
    ],
  })(KafkaLoggerTestModule);

  const moduleRef = await NestFactory.createApplicationContext(
    KafkaLoggerTestModule,
    { logger: false, abortOnError: false },
  );

  assert.ok(moduleRef.get(KafkaProducerService));
  assert.ok(moduleRef.get(OmnixysLogger));
  await moduleRef.close();
});
