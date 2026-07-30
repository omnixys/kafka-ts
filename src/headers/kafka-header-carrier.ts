import type { HeaderCarrier } from "@omnixys/observability-ts";

export class KafkaCarrier implements HeaderCarrier {
  constructor(private readonly headers: Record<string, Buffer>) {}

  get(key: string): string | undefined {
    return this.headers[key]?.toString();
  }

  set(key: string, value: string): void {
    this.headers[key] = Buffer.from(value);
  }

  keys(): string[] {
    return Object.keys(this.headers);
  }

  toKafkaHeaders(): Record<string, Buffer> {
    return this.headers;
  }
}
