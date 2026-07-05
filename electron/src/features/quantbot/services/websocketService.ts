/**
 * QuantBot WebSocket 服务（T2.2 统一后）
 *
 * 原占位实现已废弃，统一转发到 `services/websocketService.ts` 的 `websocketService` 单例。
 * 历史调用方通过本文件导入的 `websocketService` 现在拿到的是同一个带鉴权的全局单例。
 */

export {
  websocketService,
  WebSocketService,
  WebSocketStatus,
  MessageType,
  type WebSocketMessage,
  type SubscriptionConfig,
  type WebSocketErrorCallback
} from '../../../services/websocketService';

// 兼容默认导出（历史代码使用 `import websocketService from './websocketService'`）
import { websocketService as defaultService } from '../../../services/websocketService';
export default defaultService;
