/**
 * @license GPL-3.0-or-later
 * Copyright (C) 2025 Caleb Gyamfi - Omnixys Technologies
 *
 * Central Kafka topic registry for the Omnixys platform.
 *
 * This file defines all Kafka topics used across services.
 * Topics are organized by domain to keep the event system
 * consistent and type-safe.
 */

/**
 * Global Kafka topic registry.
 *
 * Structure:
 * domain → action → topic name
 *
 * Example topic:
 * ticket.delete.user
 */
export const KafkaTopics = {
  address: {
    restart: `admin.restart.address`,
    shutdown: `admin.shutdown.address`,
    createEventAddress: "event.create.address",
    deleteEventAddress: "event.delete.address",
    createUserAddresses: "authentication.create.addresses",
    deleteUserAddresses: "authentication.delete.addresses",
  },

  admin: {
    allRestart: "all.restart.admin",
    allShutdown: "all.shutdown.admin",
  },

  authentication: {
    restart: `admin.restart.authentication`,
    shutdown: `admin.shutdown.authentication`,
    deleteGuest: "invitation.deleteGuest.authentication",
    deleteGuestList: "invitation.deleteGuestList.authentication",
    createGuest: "invitation.createGuest.authentication",
  },

  event: {
    restart: `admin.restart.event`,
    shutdown: `admin.shutdown.event`,

    addRole: "authentication.addRole.event",
    removeRoles: "authentication.removeRole.event",

    delete: "authentication.delete.event",
    mediaUploaded: "event.media.uploaded",
    milestoneRecorded: "event.milestone.recorded",

    created: "event.created",
    updated: "event.updated",
    roleAssigned: "event.role.assigned",
    roleRemoved: "event.role.removed",
    roleDefinitionChanged: "event.role.definition.changed",
    userAccessChanged: "event.user.access.changed",
    ownerChanged: "event.owner.changed",
    deleted: "event.deleted",
  },

  gateway: {
    restart: `admin.restart.gateway`,
    shutdown: `admin.shutdown.gateway`,

    sendCredentials: "notification.sendCredentials.gateway",
    createWhatsappMessage: "whatsapp.message.created",
    deliveryStatus: "gateway.delivery.status",
  },

  invitation: {
    restart: `admin.restart.invitation`,
    shutdown: `admin.shutdown.invitation`,
    deleteUserInvitations: `authentication.userDelete.invitation`,
    addGuestId: `ticket.addGuestId.invitation`,
    deleteEventInvitations: "event.eventDelete.invitation",
    seatingInfoUpdated: "invitation.seating.info.updated",
  },

  notification: {
    restart: `admin.restart.notification`,
    shutdown: `admin.shutdown.notification`,
    // sendCredentials: "authentication.sendCredentials.notification",
    sendRequestReset: "authentication.sendRequestReset.notification",
    sendMagicLink: "authentication.sendMagicLink.notification",

    confirmGuest: "invitation.confirmGuest.notification",
    notifyUser: `authentication.notifyRegistration.notification`,

    eventCancelled: "event.cancel.notification",
  },

  seat: {
    restart: `admin.restart.seat`,
    shutdown: `admin.shutdown.seat`,

    create: "event.create.seat",
    delete: "event.delete.seat",

    addGuestId: `user.addGuestId.seat`,
    removeGuestId: `user.deleteGuestId.seat`,
  },

  ticket: {
    restart: `admin.restart.ticket`,
    shutdown: `admin.shutdown.ticket`,

    deleteEventTickets: "event.eventDelete.ticket",
    deleteUserTickets: "authentication.userDelete.ticket",
    create: `authentication.create.ticket`,
  },

  user: {
    restart: `admin.restart.user`,
    shutdown: `admin.shutdown.user`,
    deleteUser: "authentication.delete.user",
    createUser: "authentication.create.user",
    createGuest: "authentication.createGuest.user",
    createProviderUser: "authentication.provider.user",
    changedProjection: "user.changed.projection",
  },

  conversation: {
    guestCreated: "conversation.guest.created",
    agentReplied: "conversation.agent.replied",
    guestReplied: "conversation.guest.replied",
    chatAssigned: "conversation.chat.assigned",
    chatClosed: "conversation.chat.closed",
    channelMessage: "conversation.channel.message",
    internalCreated: "conversation.internal.created",
    internalMessage: "conversation.internal.message",
    internalRead: "conversation.internal.read",
    deliveryUpdate: "conversation.delivery.update",
    conversationMapped: "conversation.mapped",
    escalated: "conversation.escalated",
  },

  email: {
    inboundReceived: "email.inbound.received",
    outboundSend: "email.outbound.send",
    bounce: "email.bounce",
  },
} as const;

/**
 * Type representation of the KafkaTopics structure.
 */
export type KafkaTopicsType = typeof KafkaTopics;

export type KafkaTopicConfigValue = string | number | boolean;

export type KafkaTopicConfig = Record<string, KafkaTopicConfigValue>;

export type KafkaTopicPolicyName =
  | "default"
  | "retry"
  | "dlq"
  | "compacted";

export interface KafkaTopicPolicy {
  partitions: number;
  replicas: number;
  config: KafkaTopicConfig;
}

export interface KafkaTopicMetadata {
  owner?: string;
  domain?: string;
  description?: string;
  version?: number;
  producers?: readonly string[];
  consumers?: readonly string[];
  policy?: KafkaTopicPolicyName;
  partitions?: number;
  replicas?: number;
  config?: KafkaTopicConfig;
}

export interface KafkaTopicCatalogEntry {
  topic: string;
  domain: string;
  key: string;
  owner: string;
  description: string;
  version: number;
  producers: readonly string[];
  consumers: readonly string[];
  policy: KafkaTopicPolicyName;
  partitions: number;
  replicas: number;
  config: KafkaTopicConfig;
}

export interface KafkaTopicCatalog {
  defaults: KafkaTopicPolicy;
  policies: Record<KafkaTopicPolicyName, KafkaTopicPolicy>;
  topics: KafkaTopicCatalogEntry[];
}

export interface KafkaTopicValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

export const DEFAULT_KAFKA_RETRY_TOPIC_SUFFIX = ".retry";
export const DEFAULT_KAFKA_DEAD_LETTER_TOPIC_SUFFIX = ".dlq";

export interface KafkaTopicExpansionOptions {
  includeRetryTopics?: boolean;
  includeDeadLetterTopics?: boolean;
  retryTopicSuffix?: string;
  deadLetterTopicSuffix?: string;
}

export const KafkaTopicMutableConfigKeys = [
  "cleanup.policy",
  "retention.ms",
  "retention.bytes",
  "compression.type",
  "max.message.bytes",
  "segment.bytes",
] as const;

export const KafkaTopicSupportedConfigKeys = [
  ...KafkaTopicMutableConfigKeys,
  "delete.retention.ms",
  "max.compaction.lag.ms",
  "message.timestamp.type",
  "min.cleanable.dirty.ratio",
  "min.compaction.lag.ms",
  "min.insync.replicas",
] as const;

const supportedConfigKeys = new Set<string>(KafkaTopicSupportedConfigKeys);

export const KafkaTopicPolicies: Record<KafkaTopicPolicyName, KafkaTopicPolicy> = {
  default: {
    partitions: 1,
    replicas: 1,
    config: {
      "cleanup.policy": "delete",
      "compression.type": "producer",
      "retention.ms": "604800000",
    },
  },
  retry: {
    partitions: 1,
    replicas: 1,
    config: {
      "cleanup.policy": "delete",
      "compression.type": "producer",
      "retention.ms": "86400000",
    },
  },
  dlq: {
    partitions: 1,
    replicas: 1,
    config: {
      "cleanup.policy": "delete",
      "compression.type": "producer",
      "retention.ms": "2592000000",
    },
  },
  compacted: {
    partitions: 1,
    replicas: 1,
    config: {
      "cleanup.policy": "compact",
      "compression.type": "producer",
      "retention.ms": "-1",
    },
  },
} as const;

export const KafkaTopicMetadataRegistry = {
  gateway: {
    deliveryStatus: {
      owner: "gateway",
      description: "Delivery status updates from the Communication Gateway.",
      policy: "default",
      producers: ["gateway"],
      consumers: ["notification"],
    },
  },
} satisfies Partial<{
  [D in keyof KafkaTopicsType]: Partial<
    Record<keyof KafkaTopicsType[D], KafkaTopicMetadata>
  >;
}>;

/**
 * Returns all Kafka topics defined in the registry.
 * Useful for consumer subscriptions.
 */
export function getAllKafkaTopics(): string[] {
  const flatten = (obj: Record<string, unknown>): string[] =>
    Object.values(obj).flatMap((value) =>
      typeof value === "string"
        ? [value]
        : flatten(value as Record<string, unknown>),
    );

  return flatten(KafkaTopics);
}

export function getKafkaTopicCatalog(): KafkaTopicCatalog {
  const topics = Object.entries(KafkaTopics).flatMap(([domain, entries]) =>
    Object.entries(entries).map(([key, topic]) => {
      const metadata = getMetadata(domain, key);
      const policy = metadata.policy ?? inferKafkaTopicPolicy(topic);
      const policyDefaults = KafkaTopicPolicies[policy];
      const partitions = metadata.partitions ?? policyDefaults.partitions;
      const replicas = metadata.replicas ?? policyDefaults.replicas;
      const inferredOwnership = inferKafkaTopicOwnership(topic, domain);

      return {
        topic,
        domain: metadata.domain ?? domain,
        key,
        owner: metadata.owner ?? inferredOwnership.owner,
        description:
          metadata.description ?? describeKafkaTopic(topic, domain, key, policy),
        version: metadata.version ?? 1,
        producers: metadata.producers ?? inferredOwnership.producers,
        consumers: metadata.consumers ?? inferredOwnership.consumers,
        policy,
        partitions,
        replicas,
        config: {
          ...policyDefaults.config,
          ...(metadata.config ?? {}),
        },
      } satisfies KafkaTopicCatalogEntry;
    }),
  );

  topics.sort((left, right) => left.topic.localeCompare(right.topic));

  return {
    defaults: KafkaTopicPolicies.default,
    policies: KafkaTopicPolicies,
    topics,
  };
}

export function validateKafkaTopicCatalog(
  catalog: KafkaTopicCatalog = getKafkaTopicCatalog(),
): KafkaTopicValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];
  const seenTopics = new Map<string, string>();

  for (const entry of catalog.topics) {
    const path = `${entry.domain}.${entry.key}`;

    if (!entry.topic || entry.topic.trim().length === 0) {
      errors.push(`${path}: topic name must not be empty`);
      continue;
    }

    if (!isValidKafkaTopicName(entry.topic)) {
      errors.push(`${path}: invalid Kafka topic name '${entry.topic}'`);
    }

    const previousPath = seenTopics.get(entry.topic);
    if (previousPath) {
      errors.push(
        `${path}: duplicate topic '${entry.topic}' already declared at ${previousPath}`,
      );
    } else {
      seenTopics.set(entry.topic, path);
    }

    if (!Number.isInteger(entry.partitions) || entry.partitions < 1) {
      errors.push(`${path}: partitions must be a positive integer`);
    }

    if (!Number.isInteger(entry.replicas) || entry.replicas < 1) {
      errors.push(`${path}: replicas must be a positive integer`);
    }

    for (const [key, value] of Object.entries(entry.config)) {
      if (!supportedConfigKeys.has(key)) {
        errors.push(`${path}: unsupported topic config '${key}'`);
      }

      if (!isValidConfigValue(value)) {
        errors.push(`${path}: topic config '${key}' has an invalid value`);
      }
    }

    if (entry.consumers.length === 0) {
      warnings.push(`${path}: no consumers declared`);
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

export function getKafkaTopicReconciliationCatalog(
  options: KafkaTopicExpansionOptions = {},
): KafkaTopicCatalog {
  return expandKafkaTopicCatalog(getKafkaTopicCatalog(), options);
}

/**
 * Expands a list of topic names into their retry and DLQ variants.
 * Uses the same derivation logic as expandKafkaTopicCatalog().
 * This is the shared function used by both the runtime consumer
 * and the topic manager to ensure topic derivation never diverges.
 */
export function expandKafkaTopicNames(
  topics: readonly string[],
  options: KafkaTopicExpansionOptions = {},
): string[] {
  const retryTopicSuffix =
    options.retryTopicSuffix ?? DEFAULT_KAFKA_RETRY_TOPIC_SUFFIX;
  const deadLetterTopicSuffix =
    options.deadLetterTopicSuffix ?? DEFAULT_KAFKA_DEAD_LETTER_TOPIC_SUFFIX;
  const includeRetryTopics = options.includeRetryTopics ?? true;
  const includeDeadLetterTopics = options.includeDeadLetterTopics ?? false;
  const result = [...topics];
  const seen = new Set(result);

  for (const topic of topics) {
    if (
      isKafkaRetryTopic(topic, retryTopicSuffix) ||
      isKafkaDeadLetterTopic(topic, deadLetterTopicSuffix)
    ) {
      continue;
    }

    if (includeRetryTopics) {
      const retryTopic = retryKafkaTopicName(topic, retryTopicSuffix);
      if (!seen.has(retryTopic)) {
        result.push(retryTopic);
        seen.add(retryTopic);
      }
    }

    if (includeDeadLetterTopics) {
      const deadLetterTopic = deadLetterKafkaTopicName(
        topic,
        retryTopicSuffix,
        deadLetterTopicSuffix,
      );
      if (!seen.has(deadLetterTopic)) {
        result.push(deadLetterTopic);
        seen.add(deadLetterTopic);
      }
    }
  }

  return result;
}

export function expandKafkaTopicCatalog(
  catalog: KafkaTopicCatalog = getKafkaTopicCatalog(),
  options: KafkaTopicExpansionOptions = {},
): KafkaTopicCatalog {
  const retryTopicSuffix =
    options.retryTopicSuffix ?? DEFAULT_KAFKA_RETRY_TOPIC_SUFFIX;
  const deadLetterTopicSuffix =
    options.deadLetterTopicSuffix ?? DEFAULT_KAFKA_DEAD_LETTER_TOPIC_SUFFIX;
  const includeRetryTopics = options.includeRetryTopics ?? true;
  const includeDeadLetterTopics = options.includeDeadLetterTopics ?? true;
  const topics = [...catalog.topics];
  const seen = new Set(topics.map((entry) => entry.topic));

  for (const entry of catalog.topics) {
    if (
      isKafkaRetryTopic(entry.topic, retryTopicSuffix) ||
      isKafkaDeadLetterTopic(entry.topic, deadLetterTopicSuffix)
    ) {
      continue;
    }

    if (includeRetryTopics) {
      const retryTopic = retryKafkaTopicName(entry.topic, retryTopicSuffix);
      if (!seen.has(retryTopic)) {
        topics.push(createDerivedTopicEntry(entry, retryTopic, "retry"));
        seen.add(retryTopic);
      }
    }

    if (includeDeadLetterTopics) {
      const deadLetterTopic = deadLetterKafkaTopicName(
        entry.topic,
        retryTopicSuffix,
        deadLetterTopicSuffix,
      );
      if (!seen.has(deadLetterTopic)) {
        topics.push(createDerivedTopicEntry(entry, deadLetterTopic, "dlq"));
        seen.add(deadLetterTopic);
      }
    }
  }

  topics.sort((left, right) => left.topic.localeCompare(right.topic));

  return {
    ...catalog,
    topics,
  };
}

export function inferKafkaTopicPolicy(topic: string): KafkaTopicPolicyName {
  if (topic.includes(".dlq.") || topic.endsWith(".dlq")) {
    return "dlq";
  }

  if (topic.includes(".retry.") || topic.endsWith(".retry")) {
    return "retry";
  }

  if (topic.includes(".compact.") || topic.endsWith(".compacted")) {
    return "compacted";
  }

  return "default";
}

export function isKafkaRetryTopic(
  topic: string,
  retryTopicSuffix = DEFAULT_KAFKA_RETRY_TOPIC_SUFFIX,
): boolean {
  return topic.endsWith(retryTopicSuffix) || topic.includes(".retry.");
}

export function isKafkaDeadLetterTopic(
  topic: string,
  deadLetterTopicSuffix = DEFAULT_KAFKA_DEAD_LETTER_TOPIC_SUFFIX,
): boolean {
  return topic.endsWith(deadLetterTopicSuffix) || topic.includes(".dlq.");
}

export function retryKafkaTopicName(
  topic: string,
  retryTopicSuffix = DEFAULT_KAFKA_RETRY_TOPIC_SUFFIX,
): string {
  return isKafkaRetryTopic(topic, retryTopicSuffix)
    ? topic
    : `${topic}${retryTopicSuffix}`;
}

export function deadLetterKafkaTopicName(
  topic: string,
  retryTopicSuffix = DEFAULT_KAFKA_RETRY_TOPIC_SUFFIX,
  deadLetterTopicSuffix = DEFAULT_KAFKA_DEAD_LETTER_TOPIC_SUFFIX,
): string {
  if (isKafkaDeadLetterTopic(topic, deadLetterTopicSuffix)) return topic;
  if (topic.endsWith(retryTopicSuffix)) {
    return `${topic.slice(0, -retryTopicSuffix.length)}${deadLetterTopicSuffix}`;
  }
  if (topic.includes(".retry.")) {
    return topic.replace(".retry.", ".dlq.");
  }
  return `${topic}${deadLetterTopicSuffix}`;
}

export function isValidKafkaTopicName(topic: string): boolean {
  return (
    topic.length > 0 &&
    topic.length <= 249 &&
    topic !== "." &&
    topic !== ".." &&
    /^[A-Za-z0-9._-]+$/.test(topic)
  );
}

function getMetadata(domain: string, key: string): KafkaTopicMetadata {
  const registry = KafkaTopicMetadataRegistry as Record<
    string,
    Record<string, KafkaTopicMetadata> | undefined
  >;

  return registry[domain]?.[key] ?? {};
}

function inferKafkaTopicOwnership(
  topic: string,
  fallbackDomain: string,
): Pick<KafkaTopicCatalogEntry, "owner" | "producers" | "consumers"> {
  const [source, , target] = topic.split(".");
  const producer = source && source !== "all" ? source : fallbackDomain;
  const consumer = target && target !== source ? target : fallbackDomain;

  return {
    owner: consumer,
    producers: [producer],
    consumers: [consumer],
  };
}

function createDerivedTopicEntry(
  source: KafkaTopicCatalogEntry,
  topic: string,
  policy: Extract<KafkaTopicPolicyName, "retry" | "dlq">,
): KafkaTopicCatalogEntry {
  const policyDefaults = KafkaTopicPolicies[policy];
  return {
    ...source,
    topic,
    key: `${source.key}.${policy}`,
    description: `${policy.toUpperCase()} topic for ${source.topic}.`,
    producers: source.consumers,
    consumers: source.consumers,
    policy,
    partitions: policyDefaults.partitions,
    replicas: policyDefaults.replicas,
    config: {
      ...policyDefaults.config,
    },
  };
}

function describeKafkaTopic(
  topic: string,
  domain: string,
  key: string,
  policy: KafkaTopicPolicyName,
): string {
  return `${domain}.${key} Kafka topic contract for ${topic} using the ${policy} policy.`;
}

function isValidConfigValue(value: KafkaTopicConfigValue): boolean {
  if (typeof value === "string") {
    return value.trim().length > 0;
  }

  if (typeof value === "number") {
    return Number.isFinite(value);
  }

  return typeof value === "boolean";
}

/**
 * Returns a specific topic by domain and key.
 *
 * Example:
 * getTopic('invitation', 'deleteInvitation')
 */
export function getTopic<
  D extends keyof KafkaTopicsType,
  K extends keyof KafkaTopicsType[D],
>(domain: D, key: K): KafkaTopicsType[D][K] {
  return KafkaTopics[domain][key];
}
