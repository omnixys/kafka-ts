import {
  KAFKA_EVENT_METADATA,
  KAFKA_HANDLER,
} from "../core/kafka.constants.js";
import { KafkaHandlerNotFoundError } from "../core/kafka.error.js";
import type {
  KafkaPayloadType,
  KafkaTopicType,
} from "../types/kafka-event.types.js";
import type { IKafkaEventContext } from "../types/kafka-event.interface.js";
import { Injectable, OnModuleInit, Optional } from "@nestjs/common";
import { DiscoveryService, MetadataScanner, Reflector } from "@nestjs/core";
import { OmnixysLogger } from "@omnixys/logger-ts";

interface HandlerEntry {
  instance: Record<string, any>;
  methodName: string;
}

@Injectable()
export class KafkaEventDispatcherService implements OnModuleInit {
  private readonly handlers = new Map<string, HandlerEntry>();
  private readyResolver!: () => void;
  readonly ready = new Promise<void>((resolve) => {
    this.readyResolver = resolve;
  });

  constructor(
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly reflector: Reflector,
    @Optional() private readonly logger?: OmnixysLogger,
  ) {}

  onModuleInit(): void {
    this.scanHandlers();
    this.readyResolver();
    this.logger
      ?.child(KafkaEventDispatcherService.name)
      .info("Kafka handlers registered", { handlerCount: this.handlers.size });
  }

  getRegisteredTopics(): string[] {
    return [...this.handlers.keys()];
  }

  hasHandler(topic: string): boolean {
    return this.handlers.has(topic);
  }

  diagnostics() {
    return {
      handlerCount: this.handlers.size,
      topics: this.getRegisteredTopics(),
    };
  }

  async dispatch<T extends KafkaTopicType>(
    topic: T,
    payload: KafkaPayloadType<T>,
    context: IKafkaEventContext,
  ): Promise<void> {
    const entry = this.handlers.get(topic);
    if (!entry) throw new KafkaHandlerNotFoundError(String(topic));

    const method = entry.instance[entry.methodName] as (
      ...args: any[]
    ) => Promise<void> | void;
    if (typeof method !== "function") {
      throw new Error(`Invalid Kafka handler for topic "${topic}"`);
    }
    if (method.length >= 3) {
      await method.call(entry.instance, topic, payload, context);
    } else {
      await method.call(entry.instance, payload, context);
    }
  }

  private scanHandlers(): void {
    for (const wrapper of this.discovery.getProviders()) {
      const instance = wrapper.instance as Record<string, any> | undefined;
      if (!instance) continue;
      if (!this.reflector.get<boolean>(KAFKA_HANDLER, instance.constructor))
        continue;

      const prototype = Object.getPrototypeOf(instance);
      let found = false;
      for (const methodName of this.scanner.getAllMethodNames(prototype)) {
        const metadata = this.reflector.get<{ topics: string[] }>(
          KAFKA_EVENT_METADATA,
          instance[methodName],
        );
        if (!metadata) continue;
        found = true;

        for (const topic of metadata.topics) {
          const existing = this.handlers.get(topic);
          if (existing) {
            throw new Error(
              `Duplicate Kafka handler for topic "${topic}": ` +
                `${existing.instance.constructor.name}.${existing.methodName} and ` +
                `${instance.constructor.name}.${methodName}`,
            );
          }
          this.handlers.set(topic, { instance, methodName });
        }
      }

      if (!found) {
        this.logger
          ?.child(KafkaEventDispatcherService.name)
          .warn("Kafka handler class has no event methods", {
            handlerClass: instance.constructor.name,
          });
      }
    }
  }
}
