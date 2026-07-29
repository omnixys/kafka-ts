/**
 * Global Kafka event registry for the Omnixys platform.
 *
 * Maps Kafka topics to their payload types.
 */

import type {
  AnalyticsDataQualityIssue,
  AnalyticsJobEvent,
  AnalyticsProcessingEvent,
  AnalyticsResourceEvent,
  ActorIdDTO,
  AddGuestIdToInvitationDTO,
  ConversationChannelMessageDTO,
  ConversationChatAssignedDTO,
  ConversationChatClosedDTO,
  ConversationCreatedDTO,
  CreateEventAddressDTO,
  CreateGuestDTO,
  CreateSeatDTO,
  CreateUserAddressDTO,
  CreateUserProviderDTO,
  CreateUserWithInvitationIdDTO,
  DeliveryStatusDTO,
  EmailOutboundDTO,
  EmailReceivedDTO,
  EscalationDTO,
  EventCancelNotificationDTO,
  EventCreatedDTO,
  EventIdsDTO,
  EventMediaUploadedDTO,
  EventMilestoneRecordedDTO,
  EventOwnerChangedDTO,
  EventAccessDTO,
  EventRoleAssignedDTO,
  EventRoleDefinitionChangedDTO,
  EventRoleRemovedDTO,
  EventUpdatedDTO,
  GuestNotificationDTO,
  InternalConversationCreatedDTO,
  InternalMessageSentDTO,
  InternalReadReceiptDTO,
  InvitationSeatingInfoUpdatedDTO,
  LogDTO,
  SendAuthLinkDTO,
  SupportMessageReceivedDTO,
  TokenDTO,
  UserActionDTO,
  UserCredentialsDTO,
  UserIdDTO,
  UserTokenDTO,
  WhatsAppMessageDTO,
  UserIdListDTO,
  CreatePlusOneAccountDTO,
  UserProjectionChangedDTO,
} from "@omnixys/contracts";
import { KafkaTopics } from "./kafka-topics.js";

/**
 * Event payload definitions
 */
export interface KafkaEventRegistry {
  [KafkaTopics.analytics.eventsIngested]: AnalyticsProcessingEvent;
  [KafkaTopics.analytics.eventsProcessed]: AnalyticsProcessingEvent;
  [KafkaTopics.analytics.eventsQuarantined]: AnalyticsProcessingEvent;
  [KafkaTopics.analytics.dataQualityDetected]: AnalyticsDataQualityIssue;
  [KafkaTopics.analytics.identityUpdated]: AnalyticsResourceEvent;
  [KafkaTopics.analytics.sessionUpdated]: AnalyticsResourceEvent;
  [KafkaTopics.analytics.metricsUpdated]: AnalyticsResourceEvent;
  [KafkaTopics.analytics.realtimeUpdated]: AnalyticsResourceEvent;
  [KafkaTopics.analytics.ruleExecuted]: AnalyticsResourceEvent;
  [KafkaTopics.analytics.ruleFailed]: AnalyticsResourceEvent;
  [KafkaTopics.analytics.alertFired]: AnalyticsResourceEvent;
  [KafkaTopics.analytics.alertResolved]: AnalyticsResourceEvent;
  [KafkaTopics.analytics.pluginExecuted]: AnalyticsResourceEvent;
  [KafkaTopics.analytics.pluginFailed]: AnalyticsResourceEvent;
  [KafkaTopics.analytics.webhookRequested]: AnalyticsJobEvent;
  [KafkaTopics.analytics.webhookDelivered]: AnalyticsJobEvent;
  [KafkaTopics.analytics.webhookFailed]: AnalyticsJobEvent;
  [KafkaTopics.analytics.reportRequested]: AnalyticsJobEvent;
  [KafkaTopics.analytics.reportGenerated]: AnalyticsJobEvent;
  [KafkaTopics.analytics.reportFailed]: AnalyticsJobEvent;
  [KafkaTopics.analytics.exportRequested]: AnalyticsJobEvent;
  [KafkaTopics.analytics.exportCompleted]: AnalyticsJobEvent;
  [KafkaTopics.analytics.exportFailed]: AnalyticsJobEvent;
  [KafkaTopics.analytics.replayRequested]: AnalyticsJobEvent;
  [KafkaTopics.analytics.replayCompleted]: AnalyticsJobEvent;
  [KafkaTopics.analytics.replayFailed]: AnalyticsJobEvent;
  [KafkaTopics.analytics.deletionRequested]: AnalyticsJobEvent;
  [KafkaTopics.analytics.deletionCompleted]: AnalyticsJobEvent;
  [KafkaTopics.analytics.insightGenerated]: AnalyticsResourceEvent;

  [KafkaTopics.address.createEventAddress]: CreateEventAddressDTO;
  [KafkaTopics.address.deleteEventAddress]: EventIdsDTO;
  [KafkaTopics.address.createUserAddresses]: CreateUserAddressDTO;
  [KafkaTopics.address.deleteUserAddresses]: UserIdDTO;

  [KafkaTopics.address.restart]: ActorIdDTO;
  [KafkaTopics.address.shutdown]: ActorIdDTO;

  [KafkaTopics.admin.allRestart]: ActorIdDTO;
  [KafkaTopics.admin.allShutdown]: ActorIdDTO;

  [KafkaTopics.authentication.restart]: ActorIdDTO;
  [KafkaTopics.authentication.shutdown]: ActorIdDTO;
  [KafkaTopics.authentication.deleteGuest]: UserIdDTO;
  [KafkaTopics.authentication.deleteGuestList]: UserIdListDTO;
  [KafkaTopics.authentication.createGuest]: CreatePlusOneAccountDTO;

  [KafkaTopics.event.restart]: ActorIdDTO;
  [KafkaTopics.event.shutdown]: ActorIdDTO;
  [KafkaTopics.event.addRole]: CreateUserWithInvitationIdDTO;
  [KafkaTopics.event.removeRoles]: UserActionDTO;
  [KafkaTopics.event.delete]: UserIdDTO;
  [KafkaTopics.event.mediaUploaded]: EventMediaUploadedDTO;
  [KafkaTopics.event.milestoneRecorded]: EventMilestoneRecordedDTO;

  [KafkaTopics.event.created]: EventCreatedDTO;
  [KafkaTopics.event.updated]: EventUpdatedDTO;
  [KafkaTopics.event.roleAssigned]: EventRoleAssignedDTO;
  [KafkaTopics.event.roleRemoved]: EventRoleRemovedDTO;
  [KafkaTopics.event.roleDefinitionChanged]: EventRoleDefinitionChangedDTO;
  [KafkaTopics.event.userAccessChanged]: EventAccessDTO;
  [KafkaTopics.event.ownerChanged]: EventOwnerChangedDTO;
  [KafkaTopics.event.deleted]: EventIdsDTO;

  [KafkaTopics.gateway.restart]: ActorIdDTO;
  [KafkaTopics.gateway.shutdown]: ActorIdDTO;
  [KafkaTopics.gateway.sendCredentials]: UserCredentialsDTO;
  [KafkaTopics.gateway.createWhatsappMessage]: WhatsAppMessageDTO;
  [KafkaTopics.gateway.deliveryStatus]: DeliveryStatusDTO;

  [KafkaTopics.invitation.restart]: ActorIdDTO;
  [KafkaTopics.invitation.shutdown]: ActorIdDTO;
  [KafkaTopics.invitation.addGuestId]: AddGuestIdToInvitationDTO;
  [KafkaTopics.invitation.deleteUserInvitations]: UserIdDTO;
  [KafkaTopics.invitation.deleteEventInvitations]: EventIdsDTO;
  [KafkaTopics.invitation.seatingInfoUpdated]: InvitationSeatingInfoUpdatedDTO;

  [KafkaTopics.logstream.log]: LogDTO;
  [KafkaTopics.logstream.input]: LogDTO;
  [KafkaTopics.logstream.restart]: ActorIdDTO;
  [KafkaTopics.logstream.shutdown]: ActorIdDTO;

  [KafkaTopics.notification.sendRequestReset]: SendAuthLinkDTO;
  [KafkaTopics.notification.sendMagicLink]: SendAuthLinkDTO;
  [KafkaTopics.notification.notifyUser]: TokenDTO;
  [KafkaTopics.notification.restart]: ActorIdDTO;
  [KafkaTopics.notification.shutdown]: ActorIdDTO;
  [KafkaTopics.notification.confirmGuest]: GuestNotificationDTO;
  [KafkaTopics.notification.eventCancelled]: EventCancelNotificationDTO;

  [KafkaTopics.seat.create]: CreateSeatDTO;
  [KafkaTopics.seat.delete]: EventIdsDTO;
  [KafkaTopics.seat.restart]: ActorIdDTO;
  [KafkaTopics.seat.shutdown]: ActorIdDTO;
  [KafkaTopics.seat.addGuestId]: CreateUserWithInvitationIdDTO;
  [KafkaTopics.seat.removeGuestId]: UserIdDTO;

  [KafkaTopics.ticket.restart]: ActorIdDTO;
  [KafkaTopics.ticket.shutdown]: ActorIdDTO;
  [KafkaTopics.ticket.deleteUserTickets]: UserIdDTO;
  [KafkaTopics.ticket.deleteEventTickets]: EventIdsDTO;
  [KafkaTopics.ticket.create]: CreateUserWithInvitationIdDTO;

  [KafkaTopics.user.deleteUser]: UserIdDTO;
  [KafkaTopics.user.createUser]: UserTokenDTO;
  [KafkaTopics.user.createGuest]: CreateGuestDTO;
  [KafkaTopics.user.createProviderUser]: CreateUserProviderDTO;
  [KafkaTopics.user.shutdown]: ActorIdDTO;
  [KafkaTopics.user.restart]: ActorIdDTO;
  [KafkaTopics.user.changedProjection]: UserProjectionChangedDTO;

  [KafkaTopics.conversation.guestCreated]: ConversationCreatedDTO;
  [KafkaTopics.conversation.agentReplied]: SupportMessageReceivedDTO;
  [KafkaTopics.conversation.guestReplied]: SupportMessageReceivedDTO;
  [KafkaTopics.conversation.chatAssigned]: ConversationChatAssignedDTO;
  [KafkaTopics.conversation.chatClosed]: ConversationChatClosedDTO;
  [KafkaTopics.conversation.channelMessage]: ConversationChannelMessageDTO;
  [KafkaTopics.conversation.internalCreated]: InternalConversationCreatedDTO;
  [KafkaTopics.conversation.internalMessage]: InternalMessageSentDTO;
  [KafkaTopics.conversation.internalRead]: InternalReadReceiptDTO;
  [KafkaTopics.conversation.deliveryUpdate]: DeliveryStatusDTO;
  [KafkaTopics.conversation.conversationMapped]: ConversationCreatedDTO;
  [KafkaTopics.conversation.escalated]: EscalationDTO;

  [KafkaTopics.email.inboundReceived]: EmailReceivedDTO;
  [KafkaTopics.email.outboundSend]: EmailOutboundDTO;
  [KafkaTopics.email.bounce]: DeliveryStatusDTO;
}
