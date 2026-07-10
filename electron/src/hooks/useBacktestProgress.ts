/**
 * WebSocket 实时回测进度 Hook
 *
 * 用于监听后端回测进度推送，实时更新前端显示
 *
 * @example
 * ```typescript
 * const { progress, message, result, error, isConnected } = useBacktestProgress(backtestId);
 * ```
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import { SERVICE_URLS } from '../config/services';
import {
  WebSocketClient,
  ConnectionState
} from '../services/websocket/WebSocketClient';

export interface BacktestProgressData {
  type: 'connected' | 'progress' | 'result' | 'error';
  backtest_id?: string;
  progress: number;
  message: string;
  data?: unknown;
  timestamp?: number;
}

export interface UseBacktestProgressReturn {
  /** 当前进度 (0-1) */
  progress: number;
  /** 进度描述消息 */
  message: string;
  /** 回测结果（完成时） */
  result: any | null;
  /** 错误信息 */
  error: string | null;
  /** WebSocket 是否已连接 */
  isConnected: boolean;
  /** 手动重连 */
  reconnect: () => void;
  /** 手动断开 */
  disconnect: () => void;
}

/**
 * 使用 WebSocket 监听回测进度
 *
 * @param backtestId 回测任务 ID
 * @param enabled 是否启用（默认 true）
 * @param autoReconnect 是否自动重连（默认 true）
 * @param maxReconnectAttempts 最大重连次数（默认 5）
 * @returns 进度状态和控制方法
 */
export function useBacktestProgress(
  backtestId: string | null,
  enabled: boolean = true,
  autoReconnect: boolean = true,
  maxReconnectAttempts: number = 5
): UseBacktestProgressReturn {
  const [progress, setProgress] = useState(0);
  const [message, setMessage] = useState('准备开始...');
  const [result, setResult] = useState<any | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isConnected, setIsConnected] = useState(false);

  const wsRef = useRef<WebSocketClient | null>(null);
  const isManualDisconnectRef = useRef(false);

  // 连接 WebSocket
  const connect = useCallback(() => {
    if (!backtestId || !enabled || isManualDisconnectRef.current) {
      return;
    }

    // 如果已连接，先断开
    if (wsRef.current) {
      wsRef.current.disconnect();
      wsRef.current = null;
    }

    try {
      // 从环境变量或配置获取 WebSocket URL
      const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsHost = SERVICE_URLS.ENGINE_SERVICE.replace(/^http(s)?:\/\//, '');
      const wsUrl = `${wsProtocol}//${wsHost}/ws/backtest/${backtestId}`;

      console.log(`连接 WebSocket: ${wsUrl}`);

      const client = new WebSocketClient({
        url: wsUrl,
        // 自动重连由 WebSocketClient 内部驱动（指数退避 1s/2s/4s/8s/16s，最大 10s）
        reconnect: autoReconnect,
        reconnectDelay: 1000,
        maxReconnectDelay: 10000,
        maxReconnectAttempts: autoReconnect ? maxReconnectAttempts : 0,
        // 禁用心跳：后端 backtest WS 使用原始 "ping" 字符串协议，
        // 与 WebSocketClient 的 JSON 心跳不兼容
        heartbeatInterval: Number.MAX_SAFE_INTEGER,
        heartbeatTimeout: Number.MAX_SAFE_INTEGER,
      });
      wsRef.current = client;

      // 全量消息回调：接收完整消息对象（包含 type/status/progress 等顶层字段）
      client.onMessage((data) => {
        // 忽略旧 client 的回调
        if (wsRef.current !== client) return;

        try {
          console.log('收到进度更新:', data);

          const status = data.status;
          const pct = typeof data.progress === 'number' ? data.progress : data.progress === 0 ? 0 : undefined;

          if (status === 'completed') {
            setProgress(1.0);
            setMessage('回测完成');
            setResult(data.data || data);
            // 标记为完成，避免 onStateChange(DISCONNECTED) 误报重连失败
            isManualDisconnectRef.current = true;
            client.disconnect();
          } else if (status === 'failed') {
            const msg = data.message || data.error_message || '回测失败';
            setError(msg);
            setMessage(`错误: ${msg}`);
            isManualDisconnectRef.current = true;
            client.disconnect();
          } else if (pct !== undefined) {
            // 后端 progress 为 0-1
            setProgress(pct);
            setMessage(data.message || `进度: ${(pct * 100).toFixed(1)}%`);
          } else if (data.type === 'connected') {
            setMessage(data.message || '已连接');
          }
        } catch (err) {
          console.error('解析 WebSocket 消息失败:', err);
        }
      });

      client.onStateChange((state) => {
        if (wsRef.current !== client) return;

        if (state === ConnectionState.CONNECTED) {
          console.log('WebSocket 已连接');
          setIsConnected(true);
          setError(null);
        } else if (state === ConnectionState.RECONNECTING) {
          setIsConnected(false);
          setMessage('连接断开，正在重连...');
        } else if (state === ConnectionState.DISCONNECTED) {
          setIsConnected(false);
          // 仅在非手动断开 + 开启了自动重连时，视为重连耗尽
          if (!isManualDisconnectRef.current && autoReconnect) {
            setError(`重连失败，已达最大尝试次数 (${maxReconnectAttempts})`);
            setMessage('连接失败');
          }
        }
      });

      client.onError((err) => {
        if (wsRef.current !== client) return;
        console.error('WebSocket 错误:', err.message);
        setError('WebSocket 连接错误');
        setIsConnected(false);
      });

      client.connect().catch((err) => {
        console.error('WebSocket 连接失败:', err);
        setError('无法建立 WebSocket 连接');
        setIsConnected(false);
      });
    } catch (err) {
      console.error('创建 WebSocket 连接失败:', err);
      setError('无法建立 WebSocket 连接');
      setIsConnected(false);
    }
  }, [backtestId, enabled, autoReconnect, maxReconnectAttempts]);

  // 手动断开
  const disconnect = useCallback(() => {
    isManualDisconnectRef.current = true;
    if (wsRef.current) {
      wsRef.current.disconnect();
      wsRef.current = null;
    }
    setIsConnected(false);
  }, []);

  // 手动重连
  const reconnect = useCallback(() => {
    // 先断开当前连接（不依赖 disconnect() 以避免 isManualDisconnectRef 被设为 true 后阻断 connect）
    if (wsRef.current) {
      wsRef.current.disconnect();
      wsRef.current = null;
    }
    isManualDisconnectRef.current = false;

    // 延迟一下再连接，确保断开完成
    setTimeout(() => {
      connect();
    }, 100);
  }, [connect]);

  // 初始连接和清理
  useEffect(() => {
    if (backtestId && enabled) {
      isManualDisconnectRef.current = false;
      connect();
    }

    return () => {
      if (wsRef.current) {
        wsRef.current.disconnect();
        wsRef.current = null;
      }
    };
  }, [backtestId, enabled, connect]);

  return {
    progress,
    message,
    result,
    error,
    isConnected,
    reconnect,
    disconnect,
  };
}
