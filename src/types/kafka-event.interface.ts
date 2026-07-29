/**
 * @license GPL-3.0-or-later
 * Copyright (C) 2025 Caleb Gyamfi - Omnixys Technologies
 *
 * Interfaces used by Kafka event handlers.
 *
 * These types define the structure of Kafka handler functions
 * and the contextual metadata provided during event processing.
 */

/**
 * KafkaEventContext
 *
 * Metadata attached to a Kafka message when delivered to a handler.
 * This contains transport-level information coming from Kafka.
 */
export interface IKafkaEventContext {
  /** Stable envelope identity used for downstream idempotency. */
  eventId: string;
  eventVersion: string;
  eventType?: string;
  service: string;

  /**
   * Kafka topic name
   */
  topic: string;

  /**
   * Kafka partition number
   */
  partition: number;

  /**
   * Offset inside the partition
   */
  offset: string;

  /**
   * Message headers converted to string values
   */
  headers: Record<string, string | undefined>;

  /**
   * Kafka timestamp of the message
   */
  timestamp: string;

  requestId?: string;
  correlationId?: string;
  actorId?: string;
  tenantId?: string;
  traceId?: string;
}

/**
 * KafkaEventHandler
 *
 * Base interface for class-based Kafka handlers.
 */
export interface IKafkaEventHandler {
  /**
   * Called when a Kafka message is received.
   */
  handle(
    topic: string,
    payload: unknown,
    context: IKafkaEventContext,
  ): Promise<void>;
}

/**
 * KafkaEventHandlerFn
 *
 * Function-based handler signature.
 */
export type KafkaEventHandlerType = (
  topic: string,
  payload: unknown,
  context: IKafkaEventContext,
) => Promise<void> | void;

/** Compatibility alias retained for existing handler signatures. */
export type KafkaEventContext = IKafkaEventContext;
