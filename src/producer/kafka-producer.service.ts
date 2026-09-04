import { KAFKA_OPTIONS, KAFKA_PRODUCER } from "../core/kafka.constants.js";
import { KafkaLifecycleError } from "../core/kafka.error.js";
import type { KafkaModuleOptions } from "../core/kafka.options.js";
import { createKafkaHeaders } from "../headers/kafka-header-builder.js";
import type { KafkaEnvelope } from "../types/kafka-envelope.js";
import { DEFAULT_KAFKA, KAFKA_HEADERS } from "../types/kafka-constants.js";
import type {
  KafkaEventType,
  KafkaPayloadType,
  KafkaTopicType,
} from "../types/kafka-event.types.js";
import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
  Optional,
} from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { ContextAccessor } from "@omnixys/context-ts";
import { OmnixysLogger } from "@omnixys/logger-ts";
import {
  KafkaTrace,
  TraceContextExtractor,
  W3CPropagator,
} from "@omnixys/observability-ts";
import type { IHeaders, Message, Producer, TopicMessages } from "kafkajs";
import { randomUUID } from "node:crypto";

export type KafkaProducerStatus = "closing" | "ready" | "stopped";

export interface KafkaProducerDiagnostics {
  readonly status: KafkaProducerStatus;
  readonly activeSends: number;
}

export interface RawKafkaMessage {
  readonly topic: string;
  readonly value: string;
  readonly key?: string;
  readonly headers?: Record<string, string | undefined>;
}

@Injectable()
export class KafkaProducerService implements OnModuleDestroy, OnModuleInit {
  private activeSends = 0;
  private connecting?: Promise<void>;
  private connected = false;
  private closing = false;
  private closePromise?: Promise<void>;
  private readonly drainWaiters = new Set<() => void>();
  private sendQueue = Promise.resolve();

  constructor(
    @Inject(KAFKA_PRODUCER) private readonly producer: Producer,
    @Optional()
    @Inject(KAFKA_OPTIONS)
    private readonly options?: KafkaModuleOptions,
    @Optional() private readonly moduleRef?: ModuleRef,
  ) {}

  async onModuleInit(): Promise<void> {
    if (this.connecting) {
      return this.connecting;
    }
    if (this.connected) {
      return;
    }
    this.connecting = this.producer.connect().then(() => {
      this.connected = true;
      this.connecting = undefined;
    });
    return this.connecting;
  }

  send<T extends KafkaTopicType>(input: KafkaEventType<T>): Promise<void>;
  send<T extends KafkaTopicType>(
    topic: T,
    payload: KafkaPayloadType<T>,
    service?: string,
  ): Promise<void>;
  async send<T extends KafkaTopicType>(
    inputOrTopic: KafkaEventType<T> | T,
    payload?: KafkaPayloadType<T>,
    service?: string,
  ): Promise<void> {
    const input: KafkaEventType<T> =
      typeof inputOrTopic === "string"
        ? {
            topic: inputOrTopic,
            payload: payload as KafkaPayloadType<T>,
            meta: service ? { service, type: "EVENT" } : undefined,
          }
        : inputOrTopic;
    if (!ContextAccessor.get()) {
      return ContextAccessor.run({ requestId: randomUUID() }, () =>
        this.send(input),
      );
    }
    await KafkaTrace.produce(input.topic, () =>
      this.sendMessage(input.topic, this.buildMessage(input)),
    );
  }

  async sendBatch<T extends KafkaTopicType>(
    inputs: ReadonlyArray<KafkaEventType<T>>,
  ): Promise<void> {
    if (inputs.length === 0) return;
    if (!ContextAccessor.get()) {
      return ContextAccessor.run({ requestId: randomUUID() }, () =>
        this.sendBatch(inputs),
      );
    }
    const byTopic = new Map<string, Message[]>();
    await this.withSend(async () => {
      await KafkaTrace.produce("batch", async () => {
        for (const input of inputs) {
          const messages = byTopic.get(input.topic) ?? [];
          messages.push(this.buildMessage(input));
          byTopic.set(input.topic, messages);
        }
        const topicMessages: TopicMessages[] = [...byTopic].map(
          ([topic, messages]) => ({ topic, messages }),
        );
        await this.producer.sendBatch({ topicMessages, acks: -1 });
      });
      this.logger?.child(KafkaProducerService.name).debug("Kafka batch sent", {
        messageCount: inputs.length,
        topicCount: byTopic.size,
      });
    });
  }

  async rawSend(
    topic: string,
    rawValue: string,
    headers?: Record<string, string | undefined>,
  ): Promise<void> {
    await this.sendMessage(topic, {
      value: rawValue,
      headers: this.normalizeHeaders(headers),
    });
  }

  async rawSendWithHeaders(
    topic: string,
    rawValue: string,
    headers: Record<string, string | undefined>,
  ): Promise<void> {
    return this.rawSend(topic, rawValue, headers);
  }

  async rawSendBatch(messages: ReadonlyArray<RawKafkaMessage>): Promise<void> {
    if (messages.length === 0) return;
    const grouped = new Map<string, Message[]>();
    for (const message of messages) {
      const topicMessages = grouped.get(message.topic) ?? [];
      topicMessages.push({
        key: message.key,
        value: message.value,
        headers: this.normalizeHeaders(message.headers),
      });
      grouped.set(message.topic, topicMessages);
    }
    await this.withSend(() =>
      this.producer.sendBatch({
        topicMessages: [...grouped].map(([topic, values]) => ({
          topic,
          messages: values,
        })),
        acks: -1,
      }),
    );
  }

  status(): KafkaProducerStatus {
    if (this.closing) return "closing";
    return this.connected ? "ready" : "stopped";
  }

  health() {
    return { healthy: this.status() === "ready", status: this.status() };
  }

  diagnostics(): KafkaProducerDiagnostics {
    return { status: this.status(), activeSends: this.activeSends };
  }

  async drain(timeoutMs = 10_000): Promise<void> {
    if (this.activeSends === 0) return;
    await new Promise<void>((resolve, reject) => {
      const waiter = () => {
        clearTimeout(timeout);
        resolve();
      };
      const timeout = setTimeout(() => {
        this.drainWaiters.delete(waiter);
        reject(
          new KafkaLifecycleError(
            `Kafka producer drain timed out after ${timeoutMs}ms`,
            { timeoutMs, activeSends: this.activeSends },
          ),
        );
      }, timeoutMs);
      this.drainWaiters.add(waiter);
    });
  }

  flush(timeoutMs?: number): Promise<void> {
    return this.drain(timeoutMs);
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

  private buildMessage<T extends KafkaTopicType>(
    input: KafkaEventType<T>,
  ): Message {
    const context = ContextAccessor.get();
    const meta = input.meta;
    const carrier = createKafkaHeaders();
    for (const [key, value] of Object.entries(input.headers ?? {})) {
      carrier.set(key, value);
    }
    KafkaTrace.inject(carrier);
    new W3CPropagator().inject(carrier);

    const traceContext = TraceContextExtractor.current();
    const traceId = context?.trace?.traceId ?? traceContext?.traceId;
    const spanId = context?.trace?.spanId ?? traceContext?.spanId;
    const actorId = context?.principal?.actorId ?? meta?.actorId;
    const tenantId =
      context?.tenant?.tenantId ??
      context?.principal?.tenantId ??
      meta?.tenantId;

    setHeader(
      carrier,
      KAFKA_HEADERS.REQUEST_ID,
      context?.requestId ?? "unscoped",
    );
    setHeader(
      carrier,
      KAFKA_HEADERS.CORRELATION_ID,
      context?.correlationId ?? context?.requestId ?? "unscoped",
    );
    setHeader(carrier, KAFKA_HEADERS.TRACE_ID, traceId);
    setHeader(carrier, KAFKA_HEADERS.SPAN_ID, spanId);
    setHeader(carrier, KAFKA_HEADERS.ACTOR_ID, actorId);
    setHeader(carrier, KAFKA_HEADERS.USER_ID, context?.principal?.userId);
    setHeader(carrier, KAFKA_HEADERS.TENANT_ID, tenantId);
    setHeader(
      carrier,
      KAFKA_HEADERS.SERVICE,
      meta?.service ?? this.options?.serviceName,
    );
    setHeader(
      carrier,
      KAFKA_HEADERS.VERSION,
      meta?.version ?? DEFAULT_KAFKA.VERSION,
    );
    setHeader(carrier, KAFKA_HEADERS.CLASS, meta?.clazz);
    setHeader(
      carrier,
      KAFKA_HEADERS.OPERATION,
      meta?.operation ?? context?.transport.operation,
    );
    setHeader(carrier, KAFKA_HEADERS.TYPE, meta?.type ?? "EVENT");

    const envelope: KafkaEnvelope<T> = {
      eventId: input.eventId ?? randomUUID(),
      eventName: input.topic,
      eventType: meta?.type ?? "EVENT",
      eventVersion: meta?.version ?? DEFAULT_KAFKA.VERSION,
      service:
        meta?.service ??
        this.options?.serviceName ??
        DEFAULT_KAFKA.UNKNOWN_SERVICE,
      timestamp: new Date().toISOString(),
      payload: input.payload,
    };

    return {
      key: input.key,
      value: JSON.stringify(envelope),
      headers: carrier.toKafkaHeaders(),
    };
  }

  private async sendMessage(topic: string, message: Message): Promise<void> {
    await this.withSend(async () => {
      try {
        await this.producer.send({ topic, messages: [message], acks: -1 });
        this.logger
          ?.child(KafkaProducerService.name)
          .debug("Kafka message sent", {
            topic,
          });
      } catch (error) {
        this.log?.error("Kafka send failed", {
          topic,
          error,
        });
        throw error;
      }
    });
  }

  private normalizeHeaders(
    headers?: Record<string, string | undefined>,
  ): IHeaders | undefined {
    if (!headers) return undefined;
    return Object.fromEntries(
      Object.entries(headers)
        .filter((entry): entry is [string, string] => entry[1] !== undefined)
        .map(([key, value]) => [key, Buffer.from(value)]),
    );
  }

  private async withSend<T>(operation: () => Promise<T>): Promise<T> {
    if (this.closing || !this.connected) {
      this.log?.error("Kafka producer is not ready, send rejected", {
        status: this.status(),
      });
      throw new KafkaLifecycleError("Kafka producer is not ready", {
        status: this.status(),
      });
    }
    this.activeSends += 1;
    const prev = this.sendQueue;
    let release: () => void;
    this.sendQueue = new Promise<void>((resolve) => {
      release = resolve;
    });
    try {
      await prev;
      if (this.closing || !this.connected) {
        this.log?.error("Kafka producer became not ready during send", {
          status: this.status(),
        });
        throw new KafkaLifecycleError("Kafka producer is not ready", {
          status: this.status(),
        });
      }
      return await operation();
    } finally {
      release!();
      this.activeSends -= 1;
      if (this.activeSends === 0) {
        for (const waiter of this.drainWaiters) waiter();
        this.drainWaiters.clear();
      }
    }
  }

  private async closeInternal(): Promise<void> {
    this.closing = true;
    try {
      await this.drain();
    } finally {
      if (this.connected) {
        try {
          await this.producer.disconnect();
          this.logger
            ?.child(KafkaProducerService.name)
            .info("Kafka producer closed");
        } finally {
          this.connected = false;
        }
      }
      this.closing = false;
    }
  }

  private get logger(): OmnixysLogger | undefined {
    try {
      return this.moduleRef?.get(OmnixysLogger, { strict: false });
    } catch {
      return undefined;
    }
  }

  private scopedLog?: ReturnType<OmnixysLogger["log"]>;

  private get log(): ReturnType<OmnixysLogger["log"]> | undefined {
    try {
      this.scopedLog ??= this.logger?.log(
        KafkaProducerService.name,
        "package:@omnixys/kafka-ts",
      );
      return this.scopedLog;
    } catch {
      return undefined;
    }
  }
}

function setHeader(
  carrier: ReturnType<typeof createKafkaHeaders>,
  key: string,
  value: string | undefined,
): void {
  if (value !== undefined && value.length > 0) carrier.set(key, value);
}
