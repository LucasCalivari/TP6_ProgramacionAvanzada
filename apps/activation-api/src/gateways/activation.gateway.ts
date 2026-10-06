import {
  WebSocketGateway,
  WebSocketServer,
  OnGatewayConnection,
  OnGatewayDisconnect,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { Logger } from '@nestjs/common';
import { ActivationEntity } from '../entities/activation.entity';

@WebSocketGateway({
  cors: {
    origin: '*',
  },
})
export class ActivationGateway implements OnGatewayConnection, OnGatewayDisconnect {
  private readonly logger = new Logger(ActivationGateway.name);

  @WebSocketServer()
  server: Server;

  handleConnection(client: Socket) {
    this.logger.log(`Client connected: ${client.id}`);
  }

  handleDisconnect(client: Socket) {
    this.logger.log(`Client disconnected: ${client.id}`);
  }

  notifyActivationUpdate(activation: ActivationEntity, eventName = 'activation:update') {
    if (this.server) {
      this.server.emit(eventName, activation);
      this.server.emit(`activation:${activation.id}`, activation);
    }
  }
}
