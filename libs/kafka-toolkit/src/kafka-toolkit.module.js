"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaToolkitModule = void 0;
const common_1 = require("@nestjs/common");
const typeorm_1 = require("@nestjs/typeorm");
const processed_event_entity_1 = require("./entities/processed-event.entity");
const outbox_entity_1 = require("./entities/outbox.entity");
const idempotency_service_1 = require("./services/idempotency.service");
const dlq_service_1 = require("./services/dlq.service");
const outbox_service_1 = require("./services/outbox.service");
let KafkaToolkitModule = class KafkaToolkitModule {
};
exports.KafkaToolkitModule = KafkaToolkitModule;
exports.KafkaToolkitModule = KafkaToolkitModule = __decorate([
    (0, common_1.Module)({
        imports: [typeorm_1.TypeOrmModule.forFeature([processed_event_entity_1.ProcessedEventEntity, outbox_entity_1.OutboxEntity])],
        providers: [idempotency_service_1.IdempotencyService, dlq_service_1.DlqService, outbox_service_1.OutboxService],
        exports: [idempotency_service_1.IdempotencyService, dlq_service_1.DlqService, outbox_service_1.OutboxService, typeorm_1.TypeOrmModule],
    })
], KafkaToolkitModule);
//# sourceMappingURL=kafka-toolkit.module.js.map