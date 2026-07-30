export const KAFKA_OPTIONS = Symbol("KAFKA_OPTIONS");

export const KAFKA_CONSUMER = Symbol("KAFKA_CONSUMER");

/**
 * Injection token for the Kafka client instance.
 */
export const KAFKA_INSTANCE = Symbol("KAFKA_INSTANCE");

/**
 * Injection token for the Kafka producer instance.
 */
export const KAFKA_PRODUCER = Symbol("KAFKA_PRODUCER");

/** Stable, package-neutral publisher token for integrations such as logging. */
export const KAFKA_EVENT_PUBLISHER = Symbol.for(
  "@omnixys/kafka-ts/event-publisher",
);

export const KAFKA_CLIENT = Symbol("KAFKA_CLIENT");

export const KAFKA_HANDLER = Symbol("KAFKA_HANDLER");

/**
 * Metadata key used to store Kafka event topics on handler methods.
 */
export const KAFKA_EVENT_METADATA = Symbol("KAFKA_EVENT_METADATA");
