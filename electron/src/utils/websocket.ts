/**
 * WebSocket 工具（T2.2 统一后）
 *
 * 历史上这里维护了一套独立的 `WebSocketManager` + `useWebSocket` 实现，
 * T2.2 已统一收敛到底层 `services/websocket/WebSocketClient.ts`。
 *
 * 为兼容既有调用方（如 EnhancedQuickBacktest），保留以下导出：
 * - `WebSocketManager`：以 `WebSocketClient` 为底层的兼容封装（deprecated alias）
 * - `useWebSocket`：基于 `WebSocketClient` 的轻量 React Hook
 * - `ConnectionState`：从统一实现透出
 *
 * 新代码请直接使用 `services/websocket/WebSocketClient` 或
 * `services/websocketService.ts` 的 `websocketService` 单例。
 */

import { useEffect, useRef, useState, useCallback } from 'react';
import {
  WebSocketClient,
  ConnectionState,
  type WebSocketClientConfig,
  type ErrorCallback
} from '../services/websocket/WebSocketClient';

export { ConnectionState };

/** 兼容历史调用方对消息 { type, data } 结构的访问 */
export interface WebSocketMessageLike {
  type: string;
  data: Record<string, unknown>;
  [key: string]: unknown;
}

export interface WebSocketManagerConfig {
  url: string;
  onMessage?: (message: WebSocketMessageLike) => void;
  onStatusChange?: (status: ConnectionState) => void;
  onError?: (error: Error, context?: string) => void;
  reconnect?: boolean;
  debug?: boolean;
}

/**
 * 兼容性封装：内部委托给统一 `WebSocketClient`
 * @deprecated 请直接使用 `WebSocketClient` 或 `websocketService` 单例
 */
export class WebSocketManager {
  private client: WebSocketClient;
  private config: WebSocketManagerConfig;

  constructor(config: WebSocketManagerConfig) {
    this.config = config;
    const clientConfig: WebSocketClientConfig = {
      url: config.url,
      reconnect: config.reconnect ?? true,
      debug: config.debug ?? false,
      maxReconnectDelay: 60000,
      heartbeatInterval: 30000,
      heartbeatTimeout: 30000
    };
    this.client = new WebSocketClient(clientConfig);

    if (config.onMessage) {
      // 将所有 type 的消息透传给单一 onMessage 回调（兼容历史 API）
      this.client.on('message', (payload) => {
        config.onMessage?.(payload as WebSocketMessageLike);
      });
    }
    if (config.onStatusChange) {
      this.client.onStateChange(config.onStatusChange);
    }
    if (config.onError) {
      this.client.onError(config.onError as ErrorCallback);
    }
  }

  async connect(): Promise<void> {
    return this.client.connect();
  }

  disconnect(): void {
    this.client.disconnect();
  }

  send(data: unknown): boolean {
    // 历史接口接收任意 payload，统一转换为 type=message 的消息
    return this.client.send('message', data);
  }

  isConnected(): boolean {
    return this.client.getState() === ConnectionState.CONNECTED;
  }

  getStatus(): ConnectionState {
    return this.client.getState();
  }
}

export interface UseWebSocketOptions {
  reconnect?: boolean;
  debug?: boolean;
  /** 是否启用连接（false 时跳过建立连接） */
  enabled?: boolean;
  onMessage?: (message: WebSocketMessageLike) => void;
}

export interface UseWebSocketResult {
  status: ConnectionState;
  lastMessage: WebSocketMessageLike | null;
  send: (data: unknown) => boolean;
  disconnect: () => void;
  reconnect: () => Promise<void>;
  isConnected: boolean;
}

/**
 * 轻量 WebSocket Hook（基于统一 WebSocketClient）
 */
export function useWebSocket(url: string, options?: UseWebSocketOptions): UseWebSocketResult {
  const { reconnect: reconnectOpt = true, debug = false, enabled = true, onMessage } = options || {};
  const clientRef = useRef<WebSocketClient | null>(null);
  const [status, setStatus] = useState<ConnectionState>(ConnectionState.DISCONNECTED);
  const [lastMessage, setLastMessage] = useState<WebSocketMessageLike | null>(null);

  const stableOnMessageRef = useRef(onMessage);
  stableOnMessageRef.current = onMessage;

  useEffect(() => {
    if (!url || !enabled) {
      return;
    }

    const client = new WebSocketClient({
      url,
      reconnect: reconnectOpt,
      debug,
      maxReconnectDelay: 60000,
      heartbeatInterval: 30000,
      heartbeatTimeout: 30000
    });
    clientRef.current = client;

    client.onStateChange(setStatus);
    client.on('message', (payload) => {
      const msg = payload as WebSocketMessageLike;
      setLastMessage(msg);
      stableOnMessageRef.current?.(msg);
    });

    client.connect().catch(error => {
      console.error('[useWebSocket] connect failed:', error);
    });

    return () => {
      client.disconnect();
      clientRef.current = null;
    };
  }, [url, reconnectOpt, debug, enabled]);

  const send = useCallback((data: unknown): boolean => {
    if (!clientRef.current) return false;
    return clientRef.current.send('message', data);
  }, []);

  const disconnect = useCallback(() => {
    clientRef.current?.disconnect();
  }, []);

  const reconnectFn = useCallback(async (): Promise<void> => {
    if (!clientRef.current) return;
    clientRef.current.disconnect();
    // 重置 reconnect 配置后重连
    await new Promise(resolve => setTimeout(resolve, 100));
    // 直接新建 client 重连（disconnect 已清空内部状态）
    const client = new WebSocketClient({
      url,
      reconnect: true,
      debug,
      maxReconnectDelay: 60000,
      heartbeatInterval: 30000,
      heartbeatTimeout: 30000
    });
    clientRef.current = client;
    client.onStateChange(setStatus);
    client.on('message', (payload) => {
      const msg = payload as WebSocketMessageLike;
      setLastMessage(msg);
      stableOnMessageRef.current?.(msg);
    });
    await client.connect();
  }, [url, debug]);

  return {
    status,
    lastMessage,
    send,
    disconnect,
    reconnect: reconnectFn,
    isConnected: status === ConnectionState.CONNECTED
  };
}

export default WebSocketManager;
