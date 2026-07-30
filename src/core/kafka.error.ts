import { ContextAccessor } from "@omnixys/context-ts";

export class KafkaFrameworkError extends Error {
  readonly requestId: string;
  readonly correlationId: string;
  readonly traceId?: string;
  readonly actorId?: string;
  readonly tenantId?: string;

  constructor(
    readonly code: string,
    message: string,
    readonly metadata: Readonly<Record<string, unknown>> = {},
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = KafkaFrameworkError.name;
    const context = ContextAccessor.get();
    this.requestId = context?.requestId ?? "unscoped";
    this.correlationId =
      context?.correlationId ?? context?.requestId ?? "unscoped";
    this.traceId = context?.trace?.traceId;
    this.actorId = context?.principal?.actorId;
    this.tenantId = context?.tenant?.tenantId ?? context?.principal?.tenantId;
  }
}

export class KafkaEnvelopeError extends KafkaFrameworkError {
  constructor(
    metadata: Readonly<Record<string, unknown>> = {},
    options?: ErrorOptions,
  ) {
    super(
      "KAFKA_ENVELOPE_INVALID",
      "Invalid Kafka envelope",
      metadata,
      options,
    );
    this.name = KafkaEnvelopeError.name;
  }
}

export class KafkaHandlerNotFoundError extends KafkaFrameworkError {
  constructor(topic: string) {
    super(
      "KAFKA_HANDLER_NOT_FOUND",
      `No Kafka handler registered for topic "${topic}"`,
      { topic },
    );
    this.name = KafkaHandlerNotFoundError.name;
  }
}

export class KafkaLifecycleError extends KafkaFrameworkError {
  constructor(
    message: string,
    metadata: Readonly<Record<string, unknown>> = {},
  ) {
    super("KAFKA_LIFECYCLE_ERROR", message, metadata);
    this.name = KafkaLifecycleError.name;
  }
}
