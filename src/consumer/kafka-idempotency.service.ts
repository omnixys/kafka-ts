import { KAFKA_OPTIONS } from "../core/kafka.constants.js";
import type { KafkaModuleOptions } from "../core/kafka.options.js";
import { Inject, Injectable, Optional } from "@nestjs/common";
import { ValkeyService } from "@omnixys/cache-ts";
import { OmnixysLogger } from "@omnixys/logger-ts";

@Injectable()
export class KafkaIdempotencyService {
  private readonly memory = new Map<string, number>();
  private warnedAboutFallback = false;

  constructor(
    @Optional() private readonly valkey?: ValkeyService,
    @Optional()
    @Inject(KAFKA_OPTIONS)
    private readonly options?: KafkaModuleOptions,
    @Optional() private readonly logger?: OmnixysLogger,
  ) {}

  async isProcessed(eventId: string): Promise<boolean> {
    if (!eventId || this.options?.idempotency?.enabled === false) return false;
    const key = this.buildKey(eventId);
    if (this.valkey) return (await this.valkey.rawGet(key)) !== null;

    const expiresAt = this.memory.get(key);
    if (!expiresAt) return false;
    if (expiresAt <= Date.now()) {
      this.memory.delete(key);
      return false;
    }
    return true;
  }

  async markProcessed(eventId: string): Promise<void> {
    if (!eventId || this.options?.idempotency?.enabled === false) return;
    const key = this.buildKey(eventId);
    if (this.valkey) {
      await this.valkey.rawSet(key, "1", this.ttlSeconds);
      return;
    }

    this.memory.set(key, Date.now() + this.ttlSeconds * 1_000);
    if (!this.warnedAboutFallback) {
      this.warnedAboutFallback = true;
      this.logger
        ?.child(KafkaIdempotencyService.name)
        .warn("Kafka idempotency is using process-local fallback storage");
    }
  }

  diagnostics() {
    return {
      enabled: this.options?.idempotency?.enabled !== false,
      storage: this.valkey ? "valkey" : "memory",
      ttlSeconds: this.ttlSeconds,
      memoryEntries: this.memory.size,
    };
  }

  private buildKey(eventId: string): string {
    return `kafka:idempotency:${eventId}`;
  }

  private get ttlSeconds(): number {
    return this.options?.idempotency?.ttlSeconds ?? 86_400;
  }
}
