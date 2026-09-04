import { Injectable, Optional } from "@nestjs/common";
import { OmnixysLogger } from "@omnixys/logger-ts";
import { KafkaFrameworkError } from "../core/kafka.error.js";

export type KafkaCircuitState = "CLOSED" | "HALF_OPEN" | "OPEN";

interface Circuit {
  failures: number;
  lastFailureTime: number;
  state: KafkaCircuitState;
}

export class KafkaCircuitOpenError extends KafkaFrameworkError {
  constructor(readonly circuit: string) {
    super("KAFKA_CIRCUIT_OPEN", `Kafka circuit "${circuit}" is open`, {
      circuit,
    });
    this.name = KafkaCircuitOpenError.name;
  }
}

@Injectable()
export class KafkaCircuitBreakerService {
  private readonly circuits = new Map<string, Circuit>();
  private readonly log;

  constructor(@Optional() private readonly logger?: OmnixysLogger) {
    this.log = this.logger?.log(
      this.constructor.name,
      "package:@omnixys/kafka-ts",
    );
  }

  async execute<T>(
    operation: () => Promise<T>,
    circuitName = "default",
  ): Promise<T> {
    const circuit = this.getCircuit(circuitName);
    if (circuit.state === "OPEN") {
      if (Date.now() - circuit.lastFailureTime > 10_000) {
        circuit.state = "HALF_OPEN";
      } else {
        this.log?.error("Kafka circuit is open, operation rejected", {
          circuit: circuitName,
          failures: circuit.failures,
        });
        throw new KafkaCircuitOpenError(circuitName);
      }
    }

    try {
      const result = await operation();
      circuit.failures = 0;
      circuit.lastFailureTime = 0;
      circuit.state = "CLOSED";
      return result;
    } catch (error) {
      circuit.failures += 1;
      circuit.lastFailureTime = Date.now();
      if (circuit.failures >= 5) circuit.state = "OPEN";
      this.log?.error("Kafka handler circuit recorded a failure", {
        circuit: circuitName,
        failures: circuit.failures,
        error,
      });
      throw error;
    }
  }

  status(circuitName = "default"): KafkaCircuitState {
    return this.getCircuit(circuitName).state;
  }

  reset(circuitName?: string): void {
    if (circuitName) this.circuits.delete(circuitName);
    else this.circuits.clear();
  }

  diagnostics() {
    return Object.fromEntries(
      [...this.circuits].map(([name, circuit]) => [name, { ...circuit }]),
    );
  }

  private getCircuit(name: string): Circuit {
    const existing = this.circuits.get(name);
    if (existing) return existing;
    const circuit: Circuit = {
      failures: 0,
      lastFailureTime: 0,
      state: "CLOSED",
    };
    this.circuits.set(name, circuit);
    return circuit;
  }
}
