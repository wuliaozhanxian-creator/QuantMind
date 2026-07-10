/**
 * 业务层 WebSocket 单例 (T2.2 统一后)
 *
 * @deprecated 此类为历史业务封装，内部已迁移至 `WebSocketClient`
 * （services/websocket/WebSocketClient.ts）作为底层传输层。
 * 新代码请直接使用 `WebSocketClient`。
 *
 * T2.2 已统一并补齐以下能力（与底层 WebSocketClient 对齐）：
 * - 指数退避重连（1s/2s/4s/8s/16s/32s，最大 60s，无最大次数上限）
 * - 心跳超时检测（30s 未收到 pong → 主动断开并重连）
 * - 订阅去重（同一 channel/symbol 仅发送一次 subscribe）
 * - 统一错误回调（addErrorCallback / removeErrorCallback）
 *
 * 迁移说明（M4-P1-2 T2.2）：
 *   原生 WebSocket 构造已替换为 `WebSocketClient`。
 *   - 心跳/重连由 WebSocketClient 内部驱动（reconnect=false 时由本类 attemptReconnect 接管，
 *     以便在重连时刷新鉴权 URL 中的 token）。
 *   - 消息收发通过 `onMessage` / `sendRaw` 适配，保留原有 subscribe/unsubscribe 协议格式。
 */

// WebSocket连接状态
export enum WebSocketStatus {
  CONNECTING = 'connecting',
  CONNECTED = 'connected',
  DISCONNECTED = 'disconnected',
  RECONNECTING = 'reconnecting',
  ERROR = 'error'
}

// 导入服务配置
import { SERVICE_URLS } from '../config/services';
import { authService } from '../features/auth/services/authService';
import {
  WebSocketClient,
  ConnectionState
} from './websocket/WebSocketClient';

// WebSocket消息类型
export enum MessageType {
  MARKET_DATA = 'market_data',
  TRADE_SIGNAL = 'trade_signal',
  TRADE_UPDATE = 'trade_update',
  NOTIFICATION = 'notification',
  SYSTEM_ALERT = 'system_alert',
  HEARTBEAT = 'heartbeat',
  SUBSCRIBE = 'subscribe',
  UNSUBSCRIBE = 'unsubscribe'
}

// WebSocket消息接口
export interface WebSocketMessage {
  type: MessageType;
  timestamp: string;
  data: unknown;
  id?: string;
  [key: string]: unknown;
}

// 订阅配置
export interface SubscriptionConfig {
  symbols?: string[];
  channels?: string[];
  frequency?: number;
}

// 统一错误回调类型
export type WebSocketErrorCallback = (error: Error, context?: string) => void;

// 指数退避参数（T2.2）
const BASE_RECONNECT_DELAY = 1000; // 1s
const MAX_RECONNECT_DELAY = 60000; // 60s

/**
 * @deprecated 请直接使用 `WebSocketClient`（services/websocket/WebSocketClient.ts）
 */
class WebSocketService {
  /** 底层统一 WebSocket 客户端（替代原生 WebSocket 构造） */
  private client: WebSocketClient | null = null;
  private connectPromise: Promise<void> | null = null;
  private status: WebSocketStatus = WebSocketStatus.DISCONNECTED;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private authReconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private authReconnectAttempts = 0;
  private manualDisconnect = false;

  /** 已发送 subscribe 的 symbol 集合（去重） */
  private symbolSubscriptions = new Set<string>();
  /** 已发送 subscribe 的 channel 集合（去重） */
  private channelSubscriptions = new Set<string>();
  /** 业务期望订阅（即使连接未就绪也保留，重连后补发） */
  private pendingSymbols = new Set<string>();
  private pendingChannels = new Set<string>();

  private messageHandlers = new Map<MessageType, Set<(data: unknown) => void>>();
  private statusHandlers = new Set<(status: WebSocketStatus) => void>();
  private errorHandlers = new Set<WebSocketErrorCallback>();

  private getStoredToken(): string | null {
    const raw = authService.getAccessToken();
    if (!raw) return null;
    return raw.startsWith('Bearer ') ? raw.slice(7).trim() : raw.trim();
  }

  constructor(url?: string) {
    // 允许外部覆盖；默认值在 connect 时动态解析
    if (url) {
      this.customUrl = url;
    }
  }

  private customUrl?: string;

  private resolveWebSocketUrl(): string {
    return this.customUrl || SERVICE_URLS.WEBSOCKET_MARKET;
  }

  /** 统一错误回调注册（T2.2） */
  addErrorCallback(handler: WebSocketErrorCallback): void {
    this.errorHandlers.add(handler);
  }

  removeErrorCallback(handler: WebSocketErrorCallback): void {
    this.errorHandlers.delete(handler);
  }

  private emitError(error: Error, context?: string): void {
    console.warn('[WebSocketService] error:', context, error.message);
    this.errorHandlers.forEach(handler => {
      try {
        handler(error, context);
      } catch (e) {
        console.error('[WebSocketService] error handler failed:', e);
      }
    });
  }

  // 连接WebSocket
  connect(): Promise<void> {
    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.manualDisconnect = false;
    this.clearReconnectTimer();
    this.clearAuthReconnectTimer();

    let shouldClearConnectPromise = false;
    const connectPromise = new Promise<void>((resolve, _reject) => {
      const disableWebSocket =
        String((import.meta as ImportMeta).env?.VITE_DISABLE_WEBSOCKET || '').toLowerCase() === 'true';
      if (disableWebSocket) {
        this.setStatus(WebSocketStatus.DISCONNECTED);
        shouldClearConnectPromise = true;
        console.info('WebSocket已禁用，跳过连接');
        resolve();
        return;
      }

      // 已连接或正在连接则直接返回
      const curState = this.client?.getState();
      if (curState === ConnectionState.CONNECTED || curState === ConnectionState.CONNECTING) {
        shouldClearConnectPromise = true;
        resolve();
        return;
      }

      this.setStatus(WebSocketStatus.CONNECTING);

      let wsUrl = this.resolveWebSocketUrl();
      try {
        const urlObj = new URL(wsUrl);
        const token = this.getStoredToken();
        if (!token) {
          console.info('未检测到 access token，跳过 WebSocket 连接');
          this.scheduleAuthReconnect();
          shouldClearConnectPromise = true;
          resolve();
          return;
        }
        urlObj.searchParams.append('token', token);

        const tenantId = localStorage.getItem('tenant_id');
        if (tenantId) urlObj.searchParams.append('tenant_id', tenantId);

        const userStr = localStorage.getItem('user');
        if (userStr) {
          try {
            const userObj = JSON.parse(userStr);
            const userId = userObj.id || userObj.user_id;
            if (userId) urlObj.searchParams.append('user_id', String(userId));
          } catch (e) {
            // ignore
          }
        }
        wsUrl = urlObj.toString();
      } catch (e) {
        console.warn('无法解析和补充WebSocket鉴权参数:', e);
      }

      console.log('正在连接WebSocket:', wsUrl.replace(/token=[^&]+/, 'token=***'));

      // 断开旧 client（如有），避免遗留连接
      if (this.client) {
        try { this.client.disconnect(); } catch (e) { /* ignore */ }
        this.client = null;
      }

      const client = new WebSocketClient({
        url: wsUrl,
        // reconnect=false：由 WebSocketService 自行管理重连，
        // 以便在每次重连时重新解析鉴权 URL（刷新 token）。
        reconnect: false,
        reconnectDelay: BASE_RECONNECT_DELAY,
        maxReconnectDelay: MAX_RECONNECT_DELAY,
        heartbeatInterval: 30000,
        heartbeatTimeout: 30000,
      });
      this.client = client;

      // 全量消息回调：接收完整消息对象并分发
      client.onMessage((message) => {
        this.handleMessage(message);
      });

      // 状态变化：映射到 WebSocketStatus + 触发重连/补订阅
      client.onStateChange((state) => {
        // 忽略来自旧 client 的回调
        if (this.client !== client) return;

        if (state === ConnectionState.CONNECTED) {
          this.setStatus(WebSocketStatus.CONNECTED);
          this.reconnectAttempts = 0;
          this.authReconnectAttempts = 0;
          this.clearAuthReconnectTimer();
          this.connectPromise = null;
          this.resubscribeAll();
        } else if (state === ConnectionState.CONNECTING) {
          this.setStatus(WebSocketStatus.CONNECTING);
        } else if (state === ConnectionState.DISCONNECTED) {
          this.connectPromise = null;
          if (!this.manualDisconnect) {
            this.attemptReconnect();
          } else {
            this.setStatus(WebSocketStatus.DISCONNECTED);
          }
        }
      });

      // 错误回调
      client.onError((error, context) => {
        this.emitError(error, context);
      });

      client.connect()
        .then(() => {
          resolve();
        })
        .catch((error) => {
          console.error('WebSocket连接失败:', error);
          this.setStatus(WebSocketStatus.ERROR);
          this.emitError(
            error instanceof Error ? error : new Error(String(error)),
            'connect'
          );
          this.connectPromise = null;
          // 不主动 reject：交给 onclose → onStateChange(DISCONNECTED) → attemptReconnect
          resolve();
        });
    });
    this.connectPromise = shouldClearConnectPromise ? null : connectPromise;
    return connectPromise;
  }

  // 断开连接
  disconnect(): void {
    this.manualDisconnect = true;
    this.clearReconnectTimer();
    this.clearAuthReconnectTimer();
    this.authReconnectAttempts = 0;
    if (this.client) {
      this.client.disconnect();
      this.client = null;
    }
    this.connectPromise = null;
    this.setStatus(WebSocketStatus.DISCONNECTED);
  }

  // 发送消息
  send(message: WebSocketMessage): boolean {
    const disableWebSocket =
      String((import.meta as ImportMeta).env?.VITE_DISABLE_WEBSOCKET || '').toLowerCase() === 'true';
    if (disableWebSocket) return false;
    if (!this.client) return false;
    // 使用 sendRaw 以保留 subscribe/unsubscribe 消息中的顶层字段（action/symbols/topic）
    return this.client.sendRaw(message);
  }

  /**
   * 订阅数据（T2.2 去重：仅对新增 symbol/channel 发送 subscribe）
   */
  subscribe(config: SubscriptionConfig): void {
    if (config.symbols?.length) {
      const newSymbols = config.symbols.filter(s => {
        this.pendingSymbols.add(s);
        return !this.symbolSubscriptions.has(s);
      });
      if (newSymbols.length > 0) {
        newSymbols.forEach(s => this.symbolSubscriptions.add(s));
        this.send({
          type: MessageType.SUBSCRIBE,
          timestamp: new Date().toISOString(),
          data: undefined,
          action: 'subscribe',
          symbols: newSymbols,
        });
      }
    }

    if (config.channels?.length) {
      const newChannels = config.channels.filter(c => {
        this.pendingChannels.add(c);
        return !this.channelSubscriptions.has(c);
      });
      newChannels.forEach((channel) => {
        this.channelSubscriptions.add(channel);
        this.send({
          type: MessageType.SUBSCRIBE,
          timestamp: new Date().toISOString(),
          data: undefined,
          topic: channel,
        });
      });
    }
  }

  /**
   * 取消订阅（T2.2 去重：仅对已订阅项发送 unsubscribe）
   */
  unsubscribe(symbols: string[]): void {
    symbols.forEach((symbol) => {
      const key = String(symbol);
      const wasSubscribed = this.symbolSubscriptions.has(key) || this.channelSubscriptions.has(key);
      if (!wasSubscribed) {
        // 未订阅则无需发送
        this.pendingSymbols.delete(key);
        this.pendingChannels.delete(key);
        return;
      }

      let sent = false;
      if (
        key.startsWith('trade.updates.') ||
        key.startsWith('notification.') ||
        key.startsWith('stock.') ||
        key.startsWith('strategy.')
      ) {
        sent = this.send({
          type: MessageType.UNSUBSCRIBE,
          timestamp: new Date().toISOString(),
          data: undefined,
          topic: key,
        });
      }
      if (!sent) {
        sent = this.send({
          type: MessageType.UNSUBSCRIBE,
          timestamp: new Date().toISOString(),
          data: undefined,
          action: 'unsubscribe',
          symbols: [key],
        });
      }
      if (sent || wasSubscribed) {
        this.symbolSubscriptions.delete(key);
        this.channelSubscriptions.delete(key);
        this.pendingSymbols.delete(key);
        this.pendingChannels.delete(key);
      }
    });
  }

  // 添加消息处理器
  addMessageHandler(type: MessageType, handler: (data: unknown) => void): void {
    if (!this.messageHandlers.has(type)) {
      this.messageHandlers.set(type, new Set());
    }
    this.messageHandlers.get(type)!.add(handler);
  }

  // 移除消息处理器
  removeMessageHandler(type: MessageType, handler: (data: unknown) => void): void {
    this.messageHandlers.get(type)?.delete(handler);
  }

  // 添加状态变化处理器
  addStatusHandler(handler: (status: WebSocketStatus) => void): void {
    this.statusHandlers.add(handler);
  }

  // 移除状态变化处理器
  removeStatusHandler(handler: (status: WebSocketStatus) => void): void {
    this.statusHandlers.delete(handler);
  }

  // 获取当前状态
  getStatus(): WebSocketStatus {
    return this.status;
  }

  // 获取连接信息
  getConnectionInfo() {
    return {
      status: this.status,
      url: this.resolveWebSocketUrl(),
      subscriptions: [...this.symbolSubscriptions, ...this.channelSubscriptions],
      reconnectAttempts: this.reconnectAttempts,
      isConnected: this.status === WebSocketStatus.CONNECTED
    };
  }

  // 私有方法：设置状态
  private setStatus(status: WebSocketStatus): void {
    if (this.status !== status) {
      this.status = status;
      this.statusHandlers.forEach(handler => handler(status));
    }
  }

  // 私有方法：处理消息（接收 WebSocketClient.onMessage 传入的完整解析对象）
  private handleMessage(message: { type: string; data?: unknown; [key: string]: unknown }): void {
    const messageType = message.type;

    // 心跳消息由 WebSocketClient 自动处理，此处忽略
    if (messageType === MessageType.HEARTBEAT || messageType === 'pong' || messageType === 'ping') {
      return;
    }

    // 分发消息给对应的处理器
    const handlers = this.messageHandlers.get(messageType as MessageType);
    if (handlers) {
      handlers.forEach(handler => {
        try {
          handler(message.data);
        } catch (error) {
          console.error('消息处理器执行失败:', error);
          const err = error instanceof Error ? error : new Error(String(error));
          this.emitError(err, 'message-handler');
        }
      });
    }
  }

  /**
   * 指数退避重连（1s/2s/4s/8s/16s/32s，最大 60s）
   *
   * reconnect=false 时由 WebSocketService 自行管理重连，
   * 每次重连会重新解析鉴权 URL 以刷新 token。
   */
  private attemptReconnect(): void {
    const disableWebSocket =
      String((import.meta as ImportMeta).env?.VITE_DISABLE_WEBSOCKET || '').toLowerCase() === 'true';
    if (disableWebSocket) {
      this.setStatus(WebSocketStatus.DISCONNECTED);
      return;
    }
    if (this.manualDisconnect || this.reconnectTimer) {
      return;
    }
    this.reconnectAttempts++;
    this.setStatus(WebSocketStatus.RECONNECTING);

    const exp = Math.pow(2, this.reconnectAttempts - 1);
    const delay = Math.min(BASE_RECONNECT_DELAY * exp, MAX_RECONNECT_DELAY);
    console.log(`WebSocket断链，将在 ${delay}ms 后第 ${this.reconnectAttempts} 次重试`);

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect().catch(error => {
        console.error('重连失败:', error);
        const err = error instanceof Error ? error : new Error(String(error));
        this.emitError(err, 'reconnect');
      });
    }, delay);
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private scheduleAuthReconnect(): void {
    const disableWebSocket =
      String((import.meta as ImportMeta).env?.VITE_DISABLE_WEBSOCKET || '').toLowerCase() === 'true';
    if (disableWebSocket || this.manualDisconnect) {
      this.setStatus(WebSocketStatus.DISCONNECTED);
      return;
    }

    if (this.authReconnectTimer) {
      return;
    }

    this.authReconnectAttempts += 1;
    this.setStatus(WebSocketStatus.RECONNECTING);

    const delay = Math.min(BASE_RECONNECT_DELAY * Math.max(1, this.authReconnectAttempts), MAX_RECONNECT_DELAY);
    console.log(`WebSocket鉴权未就绪，将在 ${delay}ms 后第 ${this.authReconnectAttempts} 次重试`);

    this.authReconnectTimer = setTimeout(() => {
      this.authReconnectTimer = null;
      this.connect().catch(error => {
        console.error('WebSocket鉴权补连失败:', error);
        const err = error instanceof Error ? error : new Error(String(error));
        this.emitError(err, 'auth-reconnect');
      });
    }, delay);
  }

  private clearAuthReconnectTimer(): void {
    if (this.authReconnectTimer) {
      clearTimeout(this.authReconnectTimer);
      this.authReconnectTimer = null;
    }
  }

  /**
   * 重连后补发所有订阅（仅对 pending 集合中的 symbol/channel）
   */
  private resubscribeAll(): void {
    if (this.pendingSymbols.size > 0) {
      this.send({
        type: MessageType.SUBSCRIBE,
        timestamp: new Date().toISOString(),
        data: undefined,
        action: 'subscribe',
        symbols: Array.from(this.pendingSymbols),
      });
    }

    this.pendingChannels.forEach((channel) => {
      this.send({
        type: MessageType.SUBSCRIBE,
        timestamp: new Date().toISOString(),
        data: undefined,
        topic: channel,
      });
    });
  }
}

// 导出单例实例
export const websocketService = new WebSocketService();
export { WebSocketService };
