# @omnixys/kafka-ts

Kafka infrastructure module for Omnixys microservices.

This package provides a fully integrated Kafka event system for NestJS applications including:

- Kafka producer and consumer services
- Typed Kafka events
- Decorator-based event handlers
- Automatic handler discovery
- Standardized Kafka message envelope
- Trace header propagation
- Central topic registry
- Configurable Kafka module

The package is designed as a reusable infrastructure layer for the Omnixys platform.

---

# Features

- Configurable Kafka client via `KafkaModule.forRoot()`
- Typed Kafka event registry (topic → payload)
- Decorator-based event handlers
- Automatic handler discovery
- Central Kafka topic registry
- Standardized event envelope
- Trace propagation via Kafka headers
- Graceful shutdown handling
- Production-ready KafkaJS configuration

---

# Installation

```bash
pnpm add @omnixys/kafka-ts
````

---

# Basic Usage

## Register Kafka Module

```ts
import { KafkaModule } from "@omnixys/kafka-ts";

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: "invitation-service",
      brokers: ["localhost:9092"],
      groupId: "invitation-consumer",
    }),
  ],
})
export class AppModule {}
```

---

# Publishing Events

Use the `KafkaProducerService` to publish events.

```ts
import { KafkaProducerService, KafkaTopics } from "@omnixys/kafka-ts";

@Injectable()
export class InvitationPublisher {
  constructor(private readonly kafka: KafkaProducerService) {}

  async deleteInvitation(invitationId: string) {
    await this.kafka.send(
      KafkaTopics.invitation.deleteInvitation,
      {
        invitationId
      },
      "invitation-service"
    );
  }
}
```

---

# Consuming Events

Kafka event handlers are defined using decorators.

```ts
import {
  KafkaHandler,
  KafkaEvent,
  KafkaTopics,
  KafkaEventContext,
} from "@omnixys/kafka-ts";

@KafkaHandler("InvitationHandler")
export class InvitationHandler {

  @KafkaEvent(KafkaTopics.invitation.deleteInvitation)
  async handleDeleteInvitation(
    topic: string,
    payload: { invitationId: string },
    context: KafkaEventContext
  ) {
    console.log("Deleting invitation:", payload.invitationId);
  }

}
```

The handler will be automatically discovered and registered.

---

# Kafka Event Envelope

All Kafka messages follow a standardized envelope structure.

```json
{
  "event": "deleteInvitation",
  "service": "invitation-service",
  "version": "v1",
  "payload": {
    "invitationId": "abc123"
  }
}
```

This ensures consistency across services.

---

# Kafka Topics

Kafka topics are centrally defined:

```ts
export const KafkaTopics = {
  ticket: {
    deleteTickets: "ticket.delete.user"
  },

  invitation: {
    deleteInvitation: "invitation.delete.user",
    addGuestId: "invitation.addGuestId.user"
  },

  logstream: {
    log: "logstream.log.user"
  }
}
```

---

# Typed Kafka Events

The package supports typed Kafka events through an event registry.

```ts
export interface KafkaEventRegistry {
  [KafkaTopics.invitation.deleteInvitation]: {
    invitationId: string
  }
}
```

This enables compile-time validation of event payloads.

Example:

```ts
await kafka.send(
  KafkaTopics.invitation.deleteInvitation,
  {
    invitationId: "abc123"
  }
)
```

Invalid payloads will fail during TypeScript compilation.

---

# Kafka Headers

The system automatically attaches standardized Kafka headers.

Example headers:

```
x-trace-id
x-event-name
x-event-type
x-event-version
x-service
```

These headers enable:

* distributed tracing
* event metadata inspection
* debugging and observability

---

# Architecture

The internal event flow looks like this:

```
Service
   ↓
KafkaProducerService
   ↓
Kafka
   ↓
KafkaConsumerService
   ↓
KafkaEventDispatcher
   ↓
@KafkaEvent handler
```

---

# Graceful Shutdown

The Kafka module automatically disconnects producer and consumer instances when the NestJS application shuts down.

Supported signals:

* SIGINT
* SIGTERM
* app.close()

---

# License

GPL-3.0-or-later

Copyright (C) 2025 Caleb Gyamfi - Omnixys Technologies
