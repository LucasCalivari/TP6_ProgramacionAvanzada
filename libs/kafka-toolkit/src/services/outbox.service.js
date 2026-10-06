"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
var OutboxService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.OutboxService = void 0;
const common_1 = require("@nestjs/common");
const typeorm_1 = require("@nestjs/typeorm");
const typeorm_2 = require("typeorm");
const outbox_entity_1 = require("../entities/outbox.entity");
const kafkajs_1 = require("kafkajs");
let OutboxService = OutboxService_1 = class OutboxService {
    outboxRepo;
    logger = new common_1.Logger(OutboxService_1.name);
    producer;
    intervalId = null;
    isProcessing = false;
    constructor(outboxRepo) {
        this.outboxRepo = outboxRepo;
        const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
        const kafka = new kafkajs_1.Kafka({
            clientId: `${process.env.KAFKA_CLIENT_ID || 'service'}-outbox`,
            brokers,
        });
        this.producer = kafka.producer();
    }
    async onModuleInit() {
        try {
            await this.producer.connect();
            this.intervalId = setInterval(() => this.processOutbox(), 500);
        }
        catch (err) {
            this.logger.warn(`Could not start outbox processor immediately: ${err.message}`);
        }
    }
    async onModuleDestroy() {
        if (this.intervalId) {
            clearInterval(this.intervalId);
        }
        await this.producer.disconnect();
    }
    async addEvent(topic, key, payload, manager) {
        const repo = manager ? manager.getRepository(outbox_entity_1.OutboxEntity) : this.outboxRepo;
        const entry = repo.create({
            topic,
            key,
            payload,
            status: 'PENDING',
        });
        return await repo.save(entry);
    }
    async processOutbox() {
        if (this.isProcessing)
            return;
        this.isProcessing = true;
        try {
            const pendingEvents = await this.outboxRepo.find({
                where: { status: 'PENDING' },
                order: { createdAt: 'ASC' },
                take: 20,
            });
            for (const event of pendingEvents) {
                try {
                    await this.producer.send({
                        topic: event.topic,
                        messages: [
                            {
                                key: event.key,
                                value: JSON.stringify(event.payload),
                            },
                        ],
                    });
                    event.status = 'SENT';
                    event.sentAt = new Date();
                    await this.outboxRepo.save(event);
                }
                catch (err) {
                    this.logger.error(`Failed to publish outbox event ${event.id}: ${err.message}`);
                    event.status = 'FAILED';
                    event.error = err.message;
                    await this.outboxRepo.save(event);
                }
            }
        }
        catch (err) {
            this.logger.debug(`Outbox polling error: ${err.message}`);
        }
        finally {
            this.isProcessing = false;
        }
    }
};
exports.OutboxService = OutboxService;
exports.OutboxService = OutboxService = OutboxService_1 = __decorate([
    (0, common_1.Injectable)(),
    __param(0, (0, typeorm_1.InjectRepository)(outbox_entity_1.OutboxEntity)),
    __metadata("design:paramtypes", [typeorm_2.Repository])
], OutboxService);
//# sourceMappingURL=outbox.service.js.map