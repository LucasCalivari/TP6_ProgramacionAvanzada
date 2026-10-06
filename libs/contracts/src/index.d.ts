export declare const TOPICS: {
    readonly ACTIVATION_REQUESTED: "activation.requested";
    readonly BILLING_EVENTS: "billing.events";
    readonly PROVISIONING_EVENTS: "provisioning.events";
    readonly ACTIVATION_EVENTS: "activation.events";
    readonly ACTIVATION_REQUESTED_DLQ: "activation.requested.dlq";
    readonly BILLING_EVENTS_DLQ: "billing.events.dlq";
    readonly PROVISIONING_EVENTS_DLQ: "provisioning.events.dlq";
    readonly ACTIVATION_EVENTS_DLQ: "activation.events.dlq";
};
export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];
export type FailureMode = 'none' | 'billing' | 'provisioning';
export type EventType = 'ActivationRequested' | 'BillingAccountCreated' | 'BillingFailed' | 'BillingAccountCancelled' | 'ProvisioningCompleted' | 'ProvisioningFailed' | 'ActivationCompleted' | 'ActivationFailed';
export interface EventEnvelope<T = any> {
    eventId: string;
    eventType: EventType;
    version: number;
    occurredAt: string;
    correlationId: string;
    customerId: string;
    source: string;
    payload: T;
}
export interface ActivationRequestedPayload {
    planId: string;
    channel: string;
    simulateFailure?: FailureMode;
}
export interface BillingAccountCreatedPayload {
    billingAccountId: string;
    planId: string;
}
export interface BillingFailedPayload {
    reason: string;
    planId: string;
}
export interface BillingAccountCancelledPayload {
    billingAccountId?: string;
    reason: string;
}
export interface ProvisioningCompletedPayload {
    provisioningId: string;
    planId: string;
}
export interface ProvisioningFailedPayload {
    reason: string;
    planId: string;
}
export interface ActivationCompletedPayload {
    planId: string;
    billingAccountId?: string;
    provisioningId?: string;
}
export interface ActivationFailedPayload {
    reason: string;
    failedStep?: 'billing' | 'provisioning';
}
export type ActivationRequestedEvent = EventEnvelope<ActivationRequestedPayload>;
export type BillingAccountCreatedEvent = EventEnvelope<BillingAccountCreatedPayload>;
export type BillingFailedEvent = EventEnvelope<BillingFailedPayload>;
export type BillingAccountCancelledEvent = EventEnvelope<BillingAccountCancelledPayload>;
export type ProvisioningCompletedEvent = EventEnvelope<ProvisioningCompletedPayload>;
export type ProvisioningFailedEvent = EventEnvelope<ProvisioningFailedPayload>;
export type ActivationCompletedEvent = EventEnvelope<ActivationCompletedPayload>;
export type ActivationFailedEvent = EventEnvelope<ActivationFailedPayload>;
