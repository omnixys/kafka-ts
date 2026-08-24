import { KAFKA_CONSUMER, KAFKA_OPTIONS } from "../core/kafka.constants.js";
import {
  KafkaEnvelopeError,
  KafkaLifecycleError,
} from "../core/kafka.error.js";
import type { KafkaModuleOptions } from "../core/kafka.options.js";
import { KafkaEventDispatcherService } from "../dispatcher/kafka-event-dispatcher.service.js";
import { KafkaCarrier } from "../headers/kafka-header-carrier.js";
import {
  KAFKA_HEADERS,
  KAFKA_RETRY_HEADERS,
} from "../types/kafka-constants.js";
import type { KafkaEnvelope } from "../types/kafka-envelope.js";
import type {
  KafkaPayloadType,
  KafkaTopicType,
} from "../types/kafka-event.types.js";
import type { IKafkaEventContext } from "../types/kafka-event.interface.js";
import { KafkaCircuitBreakerService } from "./kafka-circuit-breaker.service.js";
import { KafkaIdempotencyService } from "./kafka-idempotency.service.js";
import { KafkaRetryService } from "./kafka-retry.service.js";
import {
  Inject,
  Injectable,
  OnApplicationBootstrap,
  OnModuleDestroy,
  Optional,
} from "@nestjs/common";
import { ContextAccessor } from "@omnixys/context-ts";
import { OmnixysLogger } from "@omnixys/logger-ts";
import { KafkaTrace } from "@omnixys/observability-ts";
import type { Consumer, EachBatchPayload, KafkaMessage } from "kafkajs";
import { randomUUID } from "node:crypto";

export type KafkaConsumerStatus =
  | "closing"
  | "idle"
  | "running"
  | "starting"
  | "stopped";

@Injectable()
export class KafkaConsumerService
  implements OnApplicationBootstrap, OnModuleDestroy
{
  private running = false;
  private starting = false;
  private connected = true;
  private closing = false;
  private started = false;
  private inFlight = 0;
  private subscribedTopics: string[] = [];
  private closePromise?: Promise<void>;
  private readonly log;

  constructor(
    @Inject(KAFKA_CONSUMER) private readonly consumer: Consumer,
    private readonly dispatcher: KafkaEventDispatcherService,
    private readonly idempotency: KafkaIdempotencyService,
    private readonly retryService: KafkaRetryService,
    private readonly circuitBreaker: KafkaCircuitBreakerService,
    @Optional()
    @Inject(KAFKA_OPTIONS)
    private readonly options?: KafkaModuleOptions,
    @Optional() private readonly logger?: OmnixysLogger,
  ) {
    this.log = this.logger?.log(this.constructor.name);
  }

  onApplicationBootstrap(): Promise<void> {
    return this.start();
  }

  async start(): Promise<void> {
    if (this.running || this.starting || !this.connected) return;
    this.starting = true;
    try {
      await this.dispatcher.ready;
      const topics = this.dispatcher.getRegisteredTopics();
      if (topics.length === 0) {
        this.started = true;
        this.logger
          ?.child(KafkaConsumerService.name)
          .warn("Kafka consumer is idle because no handlers are registered");
        return;
      }

      this.subscribedTopics = [
        ...topics,
        ...this.retryService.retryTopics(topics),
      ];
      await this.consumer.subscribe({
        topics: this.subscribedTopics,
        fromBeginning: this.options?.fromBeginning ?? false,
      });
      this.running = true;
      this.started = true;
      await this.consumer.run({
        autoCommit: false,
        eachBatch: (payload) => this.processBatch(payload),
      });
      this.logger
        ?.child(KafkaConsumerService.name)
        .info("Kafka consumer started", {
          topics: this.subscribedTopics,
        });
    } catch (error) {
      this.running = false;
      this.log?.error("Kafka consumer failed to start", { error });
      throw error;
    } finally {
      this.starting = false;
    }
  }

  ready(): boolean {
    return this.started && this.connected && !this.closing;
  }

  status(): KafkaConsumerStatus {
    if (this.closing) return "closing";
    if (this.starting) return "starting";
    if (!this.connected) return "stopped";
    return this.running ? "running" : "idle";
  }

  health() {
    const status = this.status();
    return {
      healthy: status === "running" || status === "idle",
      status,
      subscribedTopics: [...this.subscribedTopics],
    };
  }

  diagnostics() {
    return {
      status: this.status(),
      ready: this.ready(),
      inFlight: this.inFlight,
      subscribedTopics: [...this.subscribedTopics],
      retry: this.retryService.diagnostics(),
      idempotency: this.idempotency.diagnostics(),
      circuits: this.circuitBreaker.diagnostics(),
    };
  }

  async processBatch(payload: EachBatchPayload): Promise<void> {
    const {
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary,
      isRunning,
      isStale,
    } = payload;

    for (const message of batch.messages) {
      if (!isRunning() || isStale() || this.closing) break;
      await this.processMessage(
        batch.topic,
        batch.partition,
        message,
        resolveOffset,
        heartbeat,
      );
    }
    await commitOffsetsIfNecessary();
  }

  async drain(timeoutMs = 15_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.inFlight > 0) {
      if (Date.now() >= deadline) {
        this.log?.error("Kafka consumer drain timed out", {
          timeoutMs,
          inFlight: this.inFlight,
        });
        throw new KafkaLifecycleError(
          `Kafka consumer drain timed out after ${timeoutMs}ms`,
          { timeoutMs, inFlight: this.inFlight },
        );
      }
      await sleep(10);
    }
  }

  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    if (!this.connected) return Promise.resolve();
    this.closePromise = this.closeInternal();
    return this.closePromise;
  }

  shutdown(): Promise<void> {
    return this.close();
  }

  onModuleDestroy(): Promise<void> {
    return this.close();
  }

  private async processMessage(
    topic: string,
    partition: number,
    message: KafkaMessage,
    resolveOffset: (offset: string) => void,
    heartbeat: () => Promise<void>,
  ): Promise<void> {
    this.inFlight += 1;
    const headers = stringHeaders(message);
    const requestId = headers[KAFKA_HEADERS.REQUEST_ID] ?? randomUUID();
    const correlationId = headers[KAFKA_HEADERS.CORRELATION_ID] ?? requestId;
    const context = {
      requestId,
      correlationId,
      actorId: headers[KAFKA_HEADERS.ACTOR_ID],
      userId: headers[KAFKA_HEADERS.USER_ID],
      tenantId: headers[KAFKA_HEADERS.TENANT_ID],
      traceId: headers[KAFKA_HEADERS.TRACE_ID],
      spanId: headers[KAFKA_HEADERS.SPAN_ID],
      transportType: "kafka" as const,
      operation: this.retryService.originalTopic(topic, headers),
    };
    const carrier = new KafkaCarrier(bufferHeaders(message));

    try {
      await ContextAccessor.run(context, () =>
        KafkaTrace.extract(carrier, () =>
          KafkaTrace.consume(topic, () =>
            this.processMessageInContext(
              topic,
              partition,
              message,
              headers,
              resolveOffset,
              heartbeat,
            ),
          ),
        ),
      );
    } finally {
      this.inFlight -= 1;
    }
  }

  private async processMessageInContext(
    topic: string,
    partition: number,
    message: KafkaMessage,
    headers: Record<string, string | undefined>,
    resolveOffset: (offset: string) => void,
    heartbeat: () => Promise<void>,
  ): Promise<void> {
    const rawValue = message.value?.toString();
    if (!rawValue) {
      this.logger
        ?.child(KafkaConsumerService.name)
        .warn("Empty Kafka message skipped", {
          topic,
          partition,
          offset: message.offset,
        });
      resolveOffset(message.offset);
      return;
    }

    await this.waitUntilRetry(headers, heartbeat);

    let envelope: KafkaEnvelope;
    try {
      const parsed = JSON.parse(rawValue) as unknown;
      if (!isKafkaEnvelope(parsed)) {
        throw new KafkaEnvelopeError({
          topic,
          partition,
          offset: message.offset,
        });
      }
      envelope = parsed;
    } catch (error) {
      this.log?.error("Invalid Kafka message routed to retry policy", {
        topic,
        partition,
        offset: message.offset,
        error,
      });
      await this.retryService.handleRetry(topic, rawValue, headers, error);
      resolveOffset(message.offset);
      return;
    }

    const expectedTopic = this.retryService.originalTopic(topic, headers);
    if (envelope.eventName !== expectedTopic) {
      const error = new KafkaEnvelopeError({
        reason: "topic_mismatch",
        envelopeTopic: envelope.eventName,
        expectedTopic,
      });
      this.log?.error("Kafka envelope topic mismatch", {
        envelopeTopic: envelope.eventName,
        kafkaTopic: topic,
        expectedTopic,
        partition,
        offset: message.offset,
        error,
      });
      await this.retryService.handleRetry(topic, rawValue, headers, error);
      resolveOffset(message.offset);
      return;
    }

    if (
      envelope.eventId &&
      (await this.idempotency.isProcessed(envelope.eventId))
    ) {
      resolveOffset(message.offset);
      return;
    }

    const current = ContextAccessor.get();
    const eventContext: IKafkaEventContext = {
      eventId: envelope.eventId,
      eventVersion: envelope.eventVersion,
      eventType: envelope.eventType,
      service: envelope.service,
      topic,
      partition,
      offset: message.offset,
      headers,
      timestamp: message.timestamp,
      requestId: current?.requestId,
      correlationId: current?.correlationId,
      actorId: current?.principal?.actorId,
      tenantId: current?.tenant?.tenantId ?? current?.principal?.tenantId,
      traceId: current?.trace?.traceId,
    };

    try {
      await this.circuitBreaker.execute(
        () =>
          this.dispatchTyped(
            envelope.eventName,
            envelope.payload,
            eventContext,
          ),
        String(envelope.eventName),
      );
      if (envelope.eventId)
        await this.idempotency.markProcessed(envelope.eventId);
      resolveOffset(message.offset);
      await heartbeat();
    } catch (error) {
      this.log?.error("Kafka message processing failed", {
        topic,
        partition,
        offset: message.offset,
        error,
      });
      await this.retryService.handleRetry(topic, rawValue, headers, error);
      resolveOffset(message.offset);
    }
  }

  private async waitUntilRetry(
    headers: Record<string, string | undefined>,
    heartbeat: () => Promise<void>,
  ): Promise<void> {
    const retryAt = Number(headers[KAFKA_RETRY_HEADERS.RETRY_AT]);
    if (!Number.isFinite(retryAt)) return;
    while (this.running && retryAt > Date.now()) {
      await sleep(Math.min(retryAt - Date.now(), 1_000));
      await heartbeat();
    }
  }

  private dispatchTyped<T extends KafkaTopicType>(
    topic: T,
    payload: KafkaPayloadType<T>,
    context: IKafkaEventContext,
  ): Promise<void> {
    return this.dispatcher.dispatch(topic, payload, context);
  }

  private async closeInternal(): Promise<void> {
    this.closing = true;
    this.running = false;
    try {
      if (this.started && this.subscribedTopics.length > 0) {
        await this.consumer.stop();
      }
    } finally {
      try {
        await this.drain();
      } finally {
        try {
          await this.consumer.disconnect();
          this.logger
            ?.child(KafkaConsumerService.name)
            .info("Kafka consumer closed");
        } finally {
          this.connected = false;
          this.closing = false;
        }
      }
    }
  }
}

function stringHeaders(
  message: KafkaMessage,
): Record<string, string | undefined> {
  return Object.fromEntries(
    Object.entries(message.headers ?? {}).map(([key, value]) => [
      key,
      Array.isArray(value)
        ? value[0]?.toString()
        : value === undefined
          ? undefined
          : value.toString(),
    ]),
  );
}

function bufferHeaders(message: KafkaMessage): Record<string, Buffer> {
  return Object.fromEntries(
    Object.entries(message.headers ?? {}).flatMap(([key, value]) => {
      const first = Array.isArray(value) ? value[0] : value;
      if (first === undefined) return [];
      return [[key, Buffer.isBuffer(first) ? first : Buffer.from(first)]];
    }),
  );
}

function isKafkaEnvelope(value: unknown): value is KafkaEnvelope {
  if (!value || typeof value !== "object") return false;
  const envelope = value as Partial<KafkaEnvelope> & Record<string, unknown>;
  return (
    typeof envelope.eventId === "string" &&
    typeof envelope.eventName === "string" &&
    typeof envelope.eventVersion === "string" &&
    typeof envelope.service === "string" &&
    typeof envelope.timestamp === "string" &&
    Object.hasOwn(envelope, "payload")
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}
