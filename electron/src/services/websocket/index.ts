/**
 * 统一 WebSocket 服务导出 (T2.2)
 *
 * 底层 WebSocket 客户端：`WebSocketClient`
 * 业务层带鉴权的单例：`../websocketService.ts` 的 `websocketService`
 */

export { WebSocketClient, ConnectionState } from './WebSocketClient';
export type {
  WebSocketClientConfig,
  WebSocketEvent,
  EventCallback,
  StateCallback,
  ErrorCallback
} from './WebSocketClient';
