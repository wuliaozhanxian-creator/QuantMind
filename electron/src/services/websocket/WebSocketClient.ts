/**
 * 统一 WebSocket 客户端 (T2.2)
 *
 * 作为 QuantMind 前端的唯一底层 WebSocket 抽象，向上提供：
 * - 指数退避重连（1s/2s/4s/8s/16s/32s，最大 60s，无最大次数上限）
 * - 心跳超时检测（30s 未收到 pong → 主动断开并重连）
 * - 订阅去重（同一 channel 仅发送一次 subscribe）
 * - 统一错误回调（onError / offError）
 *
 * 其它历史实现（utils/websocket.ts、features/quantbot/services/websocketService.ts、
 * services/qlib/qlibBacktestService.ts 内部 WebSocketManager）均以此类为底层，
 * 或在 T2.2 中被标记为 deprecated 别名/待迁移。
 */

export enum ConnectionState {
  DISCONNECTED = 'DISCONNECTED',
  CONNECTING = 'CONNECTING',
  CONNECTED = 'CONNECTED',
  RECONNECTING = 'RECONNECTING',
  FAILED = 'FAILED'
}

export interface WebSocketClientConfig {
  url: string;
  reconnect?: boolean;
  /** 基础重连延迟（首次退避起点），默认 1000ms */
  reconnectDelay?: number;
  /** 重连最大延迟上限，默认 60000ms */
  maxReconnectDelay?: number;
  /** 最大重连次数，默认 Infinity（无限重连） */
  maxReconnectAttempts?: number;
  /** 心跳发送间隔，默认 30000ms */
  heartbeatInterval?: number;
  /** 心跳超时阈值，默认 30000ms（超过该时长未收到 pong 即判定超时） */
  heartbeatTimeout?: number;
  /** 连接超时，默认 10000ms */
  timeout?: number;
  debug?: boolean;
}

export interface WebSocketEvent {
  event: string;
  data: unknown;
  timestamp: number;
}

export type EventCallback = (data: unknown) => void;
export type StateCallback = (state: ConnectionState) => void;
export type ErrorCallback = (error: Error, context?: string) => void;

export class WebSocketClient {
  private ws: WebSocket | null = null;
  private config: Required<WebSocketClientConfig>;
  private state: ConnectionState = ConnectionState.DISCONNECTED;
  private reconnectAttempts = 0;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private heartbeatTimeoutTimer: ReturnType<typeof setTimeout> | null = null;
  private lastHeartbeatAck = 0;
  private messageQueue: Array<{ type: string; data: unknown }> = [];
  private eventHandlers = new Map<string, Set<EventCallback>>();
  private stateHandlers = new Set<StateCallback>();
  private errorHandlers = new Set<ErrorCallback>();
  /** 全量消息回调（T2.2 additive）：接收完整解析后的消息对象 */
  private messageHandlers = new Set<(message: any) => void>();
  /** channel -> 回调集合；同一 channel 只发送一次 subscribe */
  private subscriptions = new Map<string, Set<EventCallback>>();
  /** 已发送 subscribe 的 channel 集合，避免重复发送 */
  private subscribedChannels = new Set<string>();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private manualDisconnect = false;

  constructor(config: WebSocketClientConfig) {
    this.config = {
      reconnect: true,
      reconnectDelay: 1000,
      maxReconnectDelay: 60000,
      maxReconnectAttempts: Number.POSITIVE_INFINITY,
      heartbeatInterval: 30000,
      heartbeatTimeout: 30000,
      timeout: 10000,
      debug: false,
      ...config
    };
  }

  /**
   * 连接 WebSocket 服务器
   */
  async connect(): Promise<void> {
    if (this.state === ConnectionState.CONNECTED || this.state === ConnectionState.CONNECTING) {
      this.log('Already connected or connecting');
      return;
    }

    this.manualDisconnect = false;

    return new Promise((resolve, reject) => {
      this.setState(ConnectionState.CONNECTING);
      this.log(`Connecting to ${this.config.url}`);

      try {
        this.ws = new WebSocket(this.config.url);

        const timeout = setTimeout(() => {
          if (this.state === ConnectionState.CONNECTING) {
            this.ws?.close();
            const err = new Error('Connection timeout');
            this.emitError(err, 'connect-timeout');
            reject(err);
          }
        }, this.config.timeout);

        this.ws.onopen = () => {
          clearTimeout(timeout);
          this.log('Connected successfully');
          this.setState(ConnectionState.CONNECTED);
          this.reconnectAttempts = 0;
          this.lastHeartbeatAck = Date.now();
          this.startHeartbeat();
          this.flushMessageQueue();
          this.resubscribeAll();
          resolve();
        };

        this.ws.onmessage = (event) => {
          this.handleMessage(event.data);
        };

        this.ws.onclose = (event) => {
          clearTimeout(timeout);
          this.log(`Connection closed: ${event.code} ${event.reason}`);
          this.stopHeartbeat();
          this.ws = null;

          if (this.manualDisconnect) {
            this.setState(ConnectionState.DISCONNECTED);
            return;
          }

          if (this.config.reconnect && this.reconnectAttempts < this.config.maxReconnectAttempts) {
            this.attemptReconnect();
          } else {
            this.setState(ConnectionState.DISCONNECTED);
          }
        };

        this.ws.onerror = () => {
          clearTimeout(timeout);
          const err = new Error('WebSocket error');
          this.log('Connection error');
          this.emitError(err, 'ws-onerror');
          if (this.state === ConnectionState.CONNECTING) {
            reject(err);
          }
        };

      } catch (error) {
        this.log('Failed to create WebSocket:', error);
        this.setState(ConnectionState.FAILED);
        const err = error instanceof Error ? error : new Error(String(error));
        this.emitError(err, 'connect-create');
        reject(err);
      }
    });
  }

  /**
   * 断开连接
   */
  disconnect(): void {
    this.manualDisconnect = true;
    this.config.reconnect = false;
    this.stopHeartbeat();
    this.clearReconnectTimer();

    if (this.ws) {
      try {
        this.ws.close(1000, 'Client disconnect');
      } catch (e) {
        // ignore
      }
      this.ws = null;
    }

    this.setState(ConnectionState.DISCONNECTED);
    this.messageQueue = [];
  }

  /**
   * 发送消息
   */
  send(type: string, data: unknown): boolean {
    const message = {
      type,
      data,
      timestamp: Date.now()
    };

    if (this.state === ConnectionState.CONNECTED && this.ws?.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(JSON.stringify(message));
        this.log('Sent message:', type);
        return true;
      } catch (error) {
        this.log('Failed to send message:', error);
        const err = error instanceof Error ? error : new Error(String(error));
        this.emitError(err, 'send');
        this.messageQueue.push(message);
        return false;
      }
    } else {
      // 放入消息队列
      this.messageQueue.push(message);
      this.log('Message queued:', type);
      return false;
    }
  }

  /**
   * 发送原始消息（不经过 {type, data, timestamp} 包装）
   *
   * 用于兼容需要额外顶层字段（如 action / symbols / topic）的历史协议。
   * 与 send() 不同，本方法不将消息放入队列——未连接时直接返回 false。
   */
  sendRaw(message: unknown): boolean {
    if (this.state === ConnectionState.CONNECTED && this.ws?.readyState === WebSocket.OPEN) {
      try {
        const payload = typeof message === 'string' ? message : JSON.stringify(message);
        this.ws.send(payload);
        this.log('Sent raw message');
        return true;
      } catch (error) {
        this.log('Failed to send raw message:', error);
        const err = error instanceof Error ? error : new Error(String(error));
        this.emitError(err, 'send-raw');
        return false;
      }
    }
    this.log('Raw message not sent (not connected)');
    return false;
  }

  /**
   * 订阅事件
   */
  on(event: string, callback: EventCallback): void {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, new Set());
    }
    this.eventHandlers.get(event)!.add(callback);
    this.log(`Event handler registered: ${event}`);
  }

  /**
   * 取消订阅事件
   */
  off(event: string, callback?: EventCallback): void {
    if (callback) {
      this.eventHandlers.get(event)?.delete(callback);
    } else {
      this.eventHandlers.delete(event);
    }
    this.log(`Event handler removed: ${event}`);
  }

  /**
   * 订阅频道（去重：同一 channel 仅发送一次 subscribe）
   */
  subscribe(channel: string, callback: EventCallback): string {
    const subscriptionId = `${channel}_${Date.now()}_${Math.random()}`;

    if (!this.subscriptions.has(channel)) {
      this.subscriptions.set(channel, new Set());
    }
    this.subscriptions.get(channel)!.add(callback);

    // 仅在尚未订阅时发送 subscribe，避免重复请求
    if (!this.subscribedChannels.has(channel)) {
      this.subscribedChannels.add(channel);
      this.send('subscribe', { channel });
    }

    this.log(`Subscribed to channel: ${channel}`);
    return subscriptionId;
  }

  /**
   * 取消订阅频道
   */
  unsubscribe(channel: string): void {
    if (this.subscriptions.has(channel)) {
      this.subscriptions.delete(channel);
    }
    if (this.subscribedChannels.has(channel)) {
      this.subscribedChannels.delete(channel);
      this.send('unsubscribe', { channel });
      this.log(`Unsubscribed from channel: ${channel}`);
    }
  }

  /**
   * 监听连接状态变化
   */
  onStateChange(callback: StateCallback): void {
    this.stateHandlers.add(callback);
  }

  /**
   * 取消监听连接状态
   */
  offStateChange(callback: StateCallback): void {
    this.stateHandlers.delete(callback);
  }

  /**
   * 统一错误回调（T2.2）
   */
  onError(callback: ErrorCallback): void {
    this.errorHandlers.add(callback);
  }

  offError(callback: ErrorCallback): void {
    this.errorHandlers.delete(callback);
  }

  /**
   * 注册全量消息回调（T2.2 additive）
   *
   * 与 on() 不同，回调接收 **完整解析后的消息对象**（而非仅 data 字段），
   * 用于兼容后端推送 {type, status, progress, ...} 等顶层字段消息的场景。
   */
  onMessage(callback: (message: any) => void): void {
    this.messageHandlers.add(callback);
  }

  offMessage(callback: (message: any) => void): void {
    this.messageHandlers.delete(callback);
  }

  private emitError(error: Error, context?: string): void {
    this.log('Emit error:', context, error.message);
    this.errorHandlers.forEach(handler => {
      try {
        handler(error, context);
      } catch (e) {
        this.log('Error in error handler:', e);
      }
    });
  }

  /**
   * 获取当前状态
   */
  getState(): ConnectionState {
    return this.state;
  }

  /**
   * 获取连接信息
   */
  getConnectionInfo() {
    return {
      state: this.state,
      url: this.config.url,
      reconnectAttempts: this.reconnectAttempts,
      maxReconnectAttempts: this.config.maxReconnectAttempts,
      queuedMessages: this.messageQueue.length,
      subscriptions: Array.from(this.subscriptions.keys()),
      lastHeartbeat: this.lastHeartbeatAck,
      isConnected: this.state === ConnectionState.CONNECTED
    };
  }

  /**
   * 处理接收到的消息
   */
  private handleMessage(data: string): void {
    try {
      const message = JSON.parse(data);
      const { type, data: payload } = message;

      this.log('Received message:', type);

      // 处理心跳响应
      if (type === 'heartbeat' || type === 'pong') {
        this.lastHeartbeatAck = Date.now();
        this.clearHeartbeatTimeout();
        return;
      }

      // 触发事件处理器
      const handlers = this.eventHandlers.get(type);
      if (handlers) {
        handlers.forEach(handler => {
          try {
            handler(payload);
          } catch (error) {
            this.log('Error in event handler:', error);
            const err = error instanceof Error ? error : new Error(String(error));
            this.emitError(err, 'event-handler');
          }
        });
      }

      // 触发频道订阅回调
      const channelHandlers = this.subscriptions.get(type);
      if (channelHandlers) {
        channelHandlers.forEach(handler => {
          try {
            handler(payload);
          } catch (error) {
            this.log('Error in channel handler:', error);
            const err = error instanceof Error ? error : new Error(String(error));
            this.emitError(err, 'channel-handler');
          }
        });
      }

      // 触发全量消息回调（T2.2 additive）：传递完整消息对象
      this.messageHandlers.forEach(handler => {
        try {
          handler(message);
        } catch (error) {
          this.log('Error in message handler:', error);
          const err = error instanceof Error ? error : new Error(String(error));
          this.emitError(err, 'message-handler');
        }
      });

    } catch (error) {
      this.log('Failed to parse message:', error);
      const err = error instanceof Error ? error : new Error(String(error));
      this.emitError(err, 'message-parse');
    }
  }

  /**
   * 指数退避重连（1s/2s/4s/8s/16s/32s，最大 60s）
   */
  private attemptReconnect(): void {
    this.reconnectAttempts++;
    this.setState(ConnectionState.RECONNECTING);

    const exp = Math.pow(2, this.reconnectAttempts - 1);
    const delay = Math.min(
      this.config.reconnectDelay * exp,
      this.config.maxReconnectDelay
    );

    this.log(
      `Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.config.maxReconnectAttempts})`
    );

    this.clearReconnectTimer();
    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null;
      try {
        // 重新允许连接
        this.config.reconnect = true;
        await this.connect();
      } catch (error) {
        this.log('Reconnection failed:', error);
        const err = error instanceof Error ? error : new Error(String(error));
        this.emitError(err, 'reconnect');

        if (this.reconnectAttempts < this.config.maxReconnectAttempts) {
          this.attemptReconnect();
        } else {
          this.log('Max reconnection attempts reached');
          this.setState(ConnectionState.FAILED);
        }
      }
    }, delay);
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  /**
   * 开始心跳（30s 发送 + 30s 超时检测）
   */
  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.lastHeartbeatAck = Date.now();

    this.heartbeatTimer = setInterval(() => {
      if (this.state === ConnectionState.CONNECTED) {
        this.send('heartbeat', { ping: true });
        // 同时也发送 ping（兼容后端 ws_core 协议）
        this.send('ping', { ping: true });

        // 设置本次心跳超时检测
        this.clearHeartbeatTimeout();
        this.heartbeatTimeoutTimer = setTimeout(() => {
          const elapsed = Date.now() - this.lastHeartbeatAck;
          if (elapsed > this.config.heartbeatTimeout) {
            this.log(`Heartbeat timeout (${elapsed}ms without ack), reconnecting...`);
            const err = new Error(`Heartbeat timeout after ${elapsed}ms`);
            this.emitError(err, 'heartbeat-timeout');
            try {
              this.ws?.close();
            } catch (e) {
              // ignore
            }
          }
        }, this.config.heartbeatTimeout);
      }
    }, this.config.heartbeatInterval);
  }

  private clearHeartbeatTimeout(): void {
    if (this.heartbeatTimeoutTimer) {
      clearTimeout(this.heartbeatTimeoutTimer);
      this.heartbeatTimeoutTimer = null;
    }
  }

  /**
   * 停止心跳
   */
  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.clearHeartbeatTimeout();
  }

  /**
   * 刷新消息队列
   */
  private flushMessageQueue(): void {
    if (this.messageQueue.length > 0) {
      this.log(`Flushing ${this.messageQueue.length} queued messages`);

      const messages = [...this.messageQueue];
      this.messageQueue = [];

      messages.forEach(({ type, data }) => {
        this.send(type, data);
      });
    }
  }

  /**
   * 重连后重新订阅所有 channel
   */
  private resubscribeAll(): void {
    if (this.subscribedChannels.size === 0) return;
    this.log(`Resubscribing ${this.subscribedChannels.size} channels`);
    this.subscribedChannels.forEach(channel => {
      this.send('subscribe', { channel });
    });
  }

  /**
   * 设置连接状态
   */
  private setState(state: ConnectionState): void {
    if (this.state !== state) {
      const oldState = this.state;
      this.state = state;
      this.log(`State changed: ${oldState} -> ${state}`);

      this.stateHandlers.forEach(handler => {
        try {
          handler(state);
        } catch (error) {
          this.log('Error in state handler:', error);
          const err = error instanceof Error ? error : new Error(String(error));
          this.emitError(err, 'state-handler');
        }
      });
    }
  }

  /**
   * 日志输出
   */
  private log(...args: unknown[]): void {
    if (this.config.debug) {
      console.log('[WebSocketClient]', ...args);
    }
  }
}

export default WebSocketClient;
