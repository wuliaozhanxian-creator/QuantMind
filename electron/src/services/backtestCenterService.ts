/**
 * 回测中心 API 服务层
 *
 * 封装所有回测相关的API调用
 */

import axios, { AxiosInstance } from 'axios';
import { SERVICE_URLS } from '../config/services';
import { authService } from '../features/auth/services/authService';
import {
  WebSocketClient,
  ConnectionState
} from './websocket/WebSocketClient';

// API基础配置
const resolveApiBaseURL = () => `${String(SERVICE_URLS.ENGINE_SERVICE || '').replace(/\/+$/, '')}/api/v1/qlib`;
const WS_BASE_URL = SERVICE_URLS.ENGINE_SERVICE.replace(/^http(s)?:\/\//, '');

// 创建axios实例
const apiClient: AxiosInstance = axios.create({
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// 请求拦截器
apiClient.interceptors.request.use(
  (config) => {
    config.baseURL = resolveApiBaseURL();
    apiClient.defaults.baseURL = config.baseURL;
    // 可以在这里添加认证token
    const token = authService.getAccessToken();
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// 响应拦截器
apiClient.interceptors.response.use(
  (response) => response.data,
  (error) => {
    console.error('API Error:', error);
    throw error;
  }
);

// ============================================================================
// 类型定义
// ============================================================================

export interface QuickBacktestConfig {
  strategy_code: string;
  symbol: string;
  start_date: string;
  end_date: string;
  initial_capital: number;
  commission?: number;
  slippage?: number;
  user_id: string;
  engine?: 'qlib';
  qlib_provider_uri?: string;
  qlib_region?: string;
  [key: string]: any;
}

export interface BacktestResult {
  backtest_id: string;
  status: string;
  progress?: number;
  symbol?: string;
  start_date?: string;
  end_date?: string;
  initial_capital?: number;
  final_capital?: number;
  total_return?: number;
  sharpe_ratio?: number;
  max_drawdown?: number;
  win_rate?: number;
  profit_factor?: number;
  equity_curve?: Array<{ date: string; value: number }>;
  [key: string]: any;
}

export interface HistoryParams {
  user_id: string;
  page?: number;
  page_size?: number;
  sort_by?: 'created_at' | 'total_return' | 'sharpe_ratio';
  sort_order?: 'asc' | 'desc';
  status?: 'completed' | 'running' | 'failed';
}

export interface ComparisonResult {
  backtest1: BacktestResult;
  backtest2: BacktestResult;
  metrics_comparison: Array<{
    metric: string;
    value1: number;
    value2: number;
    difference: number;
    percentage_diff: number;
    better: 1 | 2 | 'equal';
  }>;
  insights?: string[];
}

export interface OptimizationConfig {
  strategy_code: string;
  symbol: string;
  start_date: string;
  end_date: string;
  initial_capital: number;
  user_id: string;
  optimization_target: 'sharpe_ratio' | 'total_return' | 'sortino_ratio' | 'calmar_ratio';
  param_ranges: Array<{
    name: string;
    type: 'float' | 'int';
    min: number;
    max: number;
    step: number;
  }>;
  max_generations?: number;
}

export interface OptimizationResult {
  optimization_id: string;
  status: string;
  progress?: number;
  best_params?: Record<string, number>;
  best_fitness?: number;
  generation_stats?: Array<{
    generation: number;
    best_fitness: number;
    avg_fitness: number;
    worst_fitness: number;
  }>;
}

export interface AdvancedAnalysis {
  backtest_id: string;
  returns_distribution: {
    bins: number[];
    frequencies: number[];
  };
  risk_metrics: {
    var_95: number;
    cvar_95: number;
    downside_deviation: number;
    skewness: number;
    kurtosis: number;
  };
  trade_statistics: {
    total_trades: number;
    win_rate: number;
    avg_holding_period: number;
    avg_win: number;
    avg_loss: number;
  };
  [key: string]: any;
}

// ============================================================================
// WebSocket进度回调
// ============================================================================

export type ProgressCallback = (progress: {
  backtest_id: string;
  status: string;
  progress: number;
  message?: string;
}) => void;

// ============================================================================
// BacktestCenterService 类
// ============================================================================

class BacktestCenterService {
  // ========== 快速回测 ==========

  async runQuickBacktest(config: QuickBacktestConfig): Promise<BacktestResult> {
    const symbols = config.symbol
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean);
    const universe = symbols.length
      ? symbols
          .map((sym) => {
            const parts = sym.split('.');
            return parts.length === 2 ? `${parts[1]}${parts[0]}` : sym;
          })
          .join(' ')
      : 'csi300';

    return apiClient.post('/backtest', {
      strategy_type: 'TopkDropout',
      strategy_params: {
        topk: 50,
        n_drop: 10,
        signal: '<PRED>',
      },
      strategy_content: config.strategy_code,
      start_date: config.start_date,
      end_date: config.end_date,
      initial_capital: config.initial_capital,
      benchmark: 'SH000300',
      universe,
      commission: config.commission ?? 0.00025,
      min_commission: 5.0,
      stamp_duty: 0.0005,
      transfer_fee: 0.00001,
      min_transfer_fee: 0.01,
      impact_cost_coefficient: 0.0005,
      user_id: config.user_id,
    });
  }

  async getBacktestResult(backtestId: string): Promise<BacktestResult> {
    return apiClient.get(`/results/${backtestId}`);
  }

  // ========== 历史管理 ==========

  async getHistory(params: HistoryParams): Promise<BacktestResult[]> {
    const { user_id, ...rest } = params;
    const data: any = await apiClient.get(`/history/${user_id}`, { params: rest });
    if (Array.isArray(data)) return data;
    if (Array.isArray(data?.backtests)) return data.backtests;
    return [];
  }

  async deleteBacktest(backtestId: string, userId?: string): Promise<void> {
    return apiClient.delete(`/results/${backtestId}`, {
      params: { user_id: userId || 'default' },
    });
  }

  async batchDelete(backtestIds: string[]): Promise<void> {
    return Promise.all(backtestIds.map(id => this.deleteBacktest(id))).then(() => {});
  }

  // ========== 策略对比 ==========

  async compareBacktests(id1: string, id2: string, userId?: string): Promise<ComparisonResult> {
    return apiClient.get(`/compare/${id1}/${id2}`, {
      params: { user_id: userId || 'default' },
    });
  }

  // ========== 参数优化 ==========

  async optimizeParameters(config: OptimizationConfig): Promise<OptimizationResult> {
    const base_request = {
      strategy_type: 'TopkDropout',
      strategy_params: {
        topk: 50,
        n_drop: 10,
        signal: '<PRED>',
      },
      strategy_content: config.strategy_code,
      start_date: config.start_date,
      end_date: config.end_date,
      initial_capital: config.initial_capital,
      benchmark: 'SH000300',
      universe: 'csi300',
      commission: 0.00025,
      min_commission: 5.0,
      stamp_duty: 0.0005,
      transfer_fee: 0.00001,
      min_transfer_fee: 0.01,
      impact_cost_coefficient: 0.0005,
      user_id: config.user_id,
    };

    return apiClient.post('/optimize', {
      base_request,
      param_ranges: config.param_ranges.map((param) => ({
        name: param.name,
        min: param.min,
        max: param.max,
        step: param.step,
      })),
      optimization_target: config.optimization_target,
      max_parallel: 5,
    });
  }

  async getOptimizationStatus(optimizationId: string): Promise<OptimizationResult> {
    return apiClient.get(`/optimization/${optimizationId}`);
  }

  // ========== 高级分析 ==========

  async getAdvancedAnalysis(backtestId: string): Promise<AdvancedAnalysis> {
    return apiClient.get(`/analysis/${backtestId}`);
  }

  // ========== 导出功能 ==========

  async exportPDF(backtestId: string): Promise<Blob> {
    const response = await axios.get(`${resolveApiBaseURL()}/export/${backtestId}/pdf`, {
      responseType: 'blob',
    });
    return response.data;
  }

  async exportExcel(backtestId: string): Promise<Blob> {
    const response = await axios.get(`${resolveApiBaseURL()}/export/${backtestId}/excel`, {
      responseType: 'blob',
    });
    return response.data;
  }

  // ========== WebSocket连接 ==========

  connectBacktestProgress(backtestId: string, onProgress: ProgressCallback): WebSocketClient {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${WS_BASE_URL}/api/v1/ws/backtest/${backtestId}`;

    const client = new WebSocketClient({
      url: wsUrl,
      // 回测进度流为短生命周期，无需自动重连
      reconnect: false,
      // 禁用心跳：后端 backtest WS 使用原始 "ping" 字符串协议，
      // 与 WebSocketClient 的 JSON 心跳不兼容
      heartbeatInterval: Number.MAX_SAFE_INTEGER,
      heartbeatTimeout: Number.MAX_SAFE_INTEGER,
    });

    // 全量消息回调：接收完整消息对象（包含 type/status/progress 等顶层字段）
    client.onMessage((data) => {
      try {
        // 后端推送 {backtest_id, status, progress, ...}
        onProgress({
          backtest_id: data.backtest_id || backtestId,
          status: data.status || 'running',
          progress: typeof data.progress === 'number' ? data.progress : 0,
          message: data.message,
        });
      } catch (error) {
        console.error('Failed to parse WebSocket message:', error);
      }
    });

    client.onStateChange((state) => {
      if (state === ConnectionState.CONNECTED) {
        console.log(`WebSocket connected for backtest ${backtestId}`);
      } else if (state === ConnectionState.DISCONNECTED) {
        console.log(`WebSocket closed for backtest ${backtestId}`);
      }
    });

    client.onError((error) => {
      console.error('WebSocket error:', error.message);
    });

    client.connect().catch((error) => {
      console.error('WebSocket connect failed:', error);
    });

    return client;
  }

  disconnectBacktestProgress(ws: WebSocketClient): void {
    if (ws) {
      ws.disconnect();
    }
  }

  // ========== 数据源管理 ==========

  async getDataSources(): Promise<any> {
    return apiClient.get('/data-sources');
  }
}

// 导出单例实例
export const backtestCenterService = new BacktestCenterService();
export default backtestCenterService;
