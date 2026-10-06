export declare class OutboxEntity {
    id: string;
    topic: string;
    key: string;
    payload: any;
    status: 'PENDING' | 'SENT' | 'FAILED';
    createdAt: Date;
    sentAt?: Date;
    error?: string;
}
