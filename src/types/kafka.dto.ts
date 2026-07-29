import type { KafkaTopics } from "./kafka-topics.js";

type DeepValueOf<T> = T extends object ? DeepValueOf<T[keyof T]> : T;

export type KafkaTopic = DeepValueOf<typeof KafkaTopics>;
