import { KAFKA_OPTIONS } from "../core/kafka.constants.js";
import type { KafkaModuleOptions } from "../core/kafka.options.js";
import { KafkaProducerService } from "../producer/kafka-producer.service.js";
import { KAFKA_RETRY_HEADERS } from "../types/kafka-constants.js";
import {
  DEFAULT_KAFKA_DEAD_LETTER_TOPIC_SUFFIX,
  DEFAULT_KAFKA_RETRY_TOPIC_SUFFIX,
  deadLetterKafkaTopicName,
  expandKafkaTopicNames,
  isKafkaDeadLetterTopic,
  isKafkaRetryTopic,
  retryKafkaTopicName,
} from "../types/kafka-topics.js";
import { Inject, Injectable, Optional } from "@nestjs/common";
import { OmnixysLogger } from "@omnixys/logger-ts";

@Injectable()
export class KafkaRetryService {
  constructor(
    private readonly producer: KafkaProducerService,
    @Optional()
    @Inject(KAFKA_OPTIONS)
    private readonly options?: KafkaModuleOptions,
    @Optional() private readonly logger?: OmnixysLogger,
  ) {}

  async handleRetry(
    topic: string,
    rawMessage: string,
    headers: Record<string, string | undefined> = {},
    error?: unknown,
  ): Promise<void> {
    const originalTopic = this.originalTopic(topic, headers);
    const retryCount = this.extractRetryCount(headers);

    if (retryCount >= this.maxRetries) {
      await this.sendToDLQ(
        originalTopic,
        rawMessage,
        headers,
        retryCount,
        error,
      );
      return;
    }

    const nextRetry = retryCount + 1;
    const delayMs = Math.min(
      this.initialDelayMs * 2 ** Math.max(0, nextRetry - 1),
      this.maxDelayMs,
    );
    const nextHeaders = {
      ...headers,
      [KAFKA_RETRY_HEADERS.COUNT]: String(nextRetry),
      [KAFKA_RETRY_HEADERS.ORIGINAL_TOPIC]: originalTopic,
      [KAFKA_RETRY_HEADERS.RETRY_AT]: String(Date.now() + delayMs),
      [KAFKA_RETRY_HEADERS.ERROR]: errorMessage(error),
    };

    await this.producer.rawSendWithHeaders(
      this.retryTopic(originalTopic),
      rawMessage,
      nextHeaders,
    );
    this.logger
      ?.child(KafkaRetryService.name)
      .warn("Kafka message scheduled for retry", {
        originalTopic,
        retryCount: nextRetry,
        maxRetries: this.maxRetries,
        delayMs,
      });
  }

  async sendToDLQ(
    originalTopic: string,
    rawMessage: string,
    headers: Record<string, string | undefined> = {},
    retries = this.extractRetryCount(headers),
    error?: unknown,
  ): Promise<void> {
    await this.producer.rawSendWithHeaders(
      this.deadLetterTopic(originalTopic),
      rawMessage,
      {
        ...headers,
        [KAFKA_RETRY_HEADERS.COUNT]: String(retries),
        [KAFKA_RETRY_HEADERS.ORIGINAL_TOPIC]: originalTopic,
        [KAFKA_RETRY_HEADERS.ERROR]:
          errorMessage(error) ?? "max-retries-exceeded",
      },
    );
    this.logger
      ?.child(KafkaRetryService.name)
      .error("Kafka message sent to DLQ", {
        originalTopic,
        retries,
      });
  }

  retryTopic(topic: string): string {
    return retryKafkaTopicName(topic, this.retryTopicSuffix);
  }

  deadLetterTopic(topic: string): string {
    return deadLetterKafkaTopicName(
      topic,
      this.retryTopicSuffix,
      this.deadLetterTopicSuffix,
    );
  }

  retryTopics(topics: readonly string[]): string[] {
    const expanded = expandKafkaTopicNames(topics, {
      includeRetryTopics: true,
      includeDeadLetterTopics: false,
      retryTopicSuffix: this.retryTopicSuffix,
      deadLetterTopicSuffix: this.deadLetterTopicSuffix,
    });
    const originals = new Set(topics);
    return expanded.filter((topic) => !originals.has(topic));
  }

  originalTopic(
    topic: string,
    headers: Record<string, string | undefined> = {},
  ): string {
    const explicit = headers[KAFKA_RETRY_HEADERS.ORIGINAL_TOPIC];
    if (explicit) return explicit;
    return topic.endsWith(this.retryTopicSuffix)
      ? topic.slice(0, -this.retryTopicSuffix.length)
      : topic;
  }

  diagnostics() {
    return {
      maxRetries: this.maxRetries,
      initialDelayMs: this.initialDelayMs,
      maxDelayMs: this.maxDelayMs,
      retryTopicSuffix: this.retryTopicSuffix,
      deadLetterTopicSuffix: this.deadLetterTopicSuffix,
    };
  }

  private extractRetryCount(
    headers: Record<string, string | undefined>,
  ): number {
    const parsed = Number(headers[KAFKA_RETRY_HEADERS.COUNT] ?? 0);
    return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
  }

  private get maxRetries(): number {
    return this.options?.retry?.maxRetries ?? 5;
  }

  private get initialDelayMs(): number {
    return this.options?.retry?.initialDelayMs ?? 1_000;
  }

  private get maxDelayMs(): number {
    return this.options?.retry?.maxDelayMs ?? 60_000;
  }

  private get retryTopicSuffix(): string {
    return (
      this.options?.retry?.retryTopicSuffix ?? DEFAULT_KAFKA_RETRY_TOPIC_SUFFIX
    );
  }

  private get deadLetterTopicSuffix(): string {
    return (
      this.options?.retry?.deadLetterTopicSuffix ??
      DEFAULT_KAFKA_DEAD_LETTER_TOPIC_SUFFIX
    );
  }
}

function errorMessage(error: unknown): string | undefined {
  if (error === undefined) return undefined;
  const message = error instanceof Error ? error.message : String(error);
  return message.slice(0, 512);
}
