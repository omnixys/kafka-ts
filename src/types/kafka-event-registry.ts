/**
 * Global Kafka event registry for the Omnixys platform.
 *
 * Maps Kafka topics to their payload types.
 */

import type {
  ActorIdDTO,
  AddGuestIdToInvitationDTO,
  CreateEventAddressDTO,
  CreateGuestDTO,
  CreateSeatDTO,
  CreateUserAddressDTO,
  CreateUserProviderDTO,
  CreateUserWithInvitationIdDTO,
  EventCancelNotificationDTO,
  EventCreatedDTO,
  EventIdsDTO,
  EventMediaUploadedDTO,
  EventMilestoneRecordedDTO,
  EventOwnerChangedDTO,
  EventRoleAssignedDTO,
  EventRoleRemovedDTO,
  EventUpdatedDTO,
  GuestNotificationDTO,
  InvitationSeatingInfoUpdatedDTO,
  SendAuthLinkDTO,
  TokenDTO,
  UserActionDTO,
  UserCredentialsDTO,
  UserIdDTO,
  UserTokenDTO,
  WhatsAppDLQDTO,
  WhatsAppMessageDTO,
  WhatsappOutgoingDTO,
  LogDTO,
  UserIdListDTO,
  CreatePlusOneAccountDTO,
} from "@omnixys/contracts";
import { KafkaTopics } from "./kafka-topics.js";

/**
 * Event payload definitions
 */
export interface KafkaEventRegistry {
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
  [KafkaTopics.event.ownerChanged]: EventOwnerChangedDTO;
  [KafkaTopics.event.deleted]: EventIdsDTO;

  [KafkaTopics.gateway.restart]: ActorIdDTO;
  [KafkaTopics.gateway.shutdown]: ActorIdDTO;
  [KafkaTopics.gateway.sendCredentials]: UserCredentialsDTO;
  [KafkaTopics.gateway.createWhatsappMessage]: WhatsAppMessageDTO;

  [KafkaTopics.invitation.restart]: ActorIdDTO;
  [KafkaTopics.invitation.shutdown]: ActorIdDTO;
  [KafkaTopics.invitation.addGuestId]: AddGuestIdToInvitationDTO;
  [KafkaTopics.invitation.deleteUserInvitations]: UserIdDTO;
  [KafkaTopics.invitation.deleteEventInvitations]: EventIdsDTO;
  [KafkaTopics.invitation.seatingInfoUpdated]: InvitationSeatingInfoUpdatedDTO;

  [KafkaTopics.logstream.log]: LogDTO;
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

  [KafkaTopics.whatsapp.outgoing]: WhatsappOutgoingDTO;
  [KafkaTopics.whatsapp.retry]: WhatsappOutgoingDTO;
  [KafkaTopics.whatsapp.dlq]: WhatsAppDLQDTO;
}
