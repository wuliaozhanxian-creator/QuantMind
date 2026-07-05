/**
 * 全局状态类型定义 (T2.3 收敛)
 *
 * 历史上 Recoil 的 atom 定义与类型混放在 `state/atoms.ts` / `state/enhancedAtoms.ts`。
 * 经排查，所有 Recoil atom 均为死代码（没有任何 `useRecoilState` / `useRecoilValue` 消费），
 * 仅有少量类型（`DashboardTab` / `Strategy` 等）被外部引用。
 *
 * T2.3 收敛策略：
 * - 移除 Recoil 依赖与全部 atom 定义（死代码清理）
 * - 类型集中到本文件 `state/types.ts`
 * - `state/atoms.ts` 改为转发导出，保持既有引用路径可用
 * - `App.tsx` 移除 `RecoilRoot`
 * - 业务状态管理统一为：Redux Toolkit（全局 app state）+ Zustand（feature-scoped stores）
 */

import type {
  StrategyParams as IStrategyParams,
  ChatMessage as IChatMessage,
  Strategy as IStrategy
} from '../types/strategy';

export type DashboardTab =
  | 'dashboard'
  | 'strategy'
  | 'backtest'
  | 'trading'
  | 'notifications'
  | 'community'
  | 'profile'
  | 'admin';

// AI策略相关状态 - 使用统一的类型定义
export type StrategyParams = IStrategyParams;
export type ChatMessage = IChatMessage;
export type Strategy = IStrategy;

export interface BacktestResult {
  id: string;
  strategyId: string;
  performance: {
    totalReturn: number;
    annualizedReturn: number;
    maxDrawdown: number;
    sharpeRatio: number;
    winRate: number;
    profitFactor: number;
  };
  trades: unknown[];
  equity: unknown[];
  charts: {
    equity: string;
    drawdown: string;
    returns: string;
  };
}

// ==================== 模板相关类型 ====================

export interface StrategyTemplate {
  id: string;
  name: string;
  description: string;
  category: string;
  riskLevel: 'low' | 'medium' | 'high';
  complexity: 'low' | 'medium' | 'high';
  market: string;
  minCapital: number;
  maxSymbols: number;
  requiredParams: string[];
  optionalParams: string[];
  codeTemplate: string;
  tags: string[];
  metadata: {
    author: string;
    version: string;
    createdAt: string;
    updatedAt: string;
    usage_count: number;
    rating: number;
  };
}

export interface TemplateMatch {
  template: StrategyTemplate;
  matchScore: number;
  compatibilityScore: number;
  suggestions: string[];
  requiredModifications: string[];
}

// ==================== 验证相关类型 ====================

export interface ValidationError {
  field: string;
  rule: string;
  message: string;
  severity: 'error' | 'warning' | 'info';
  suggestion?: string;
}

export interface ValidationResult {
  isValid: boolean;
  score?: number;
  errors: ValidationError[];
  warnings: ValidationError[];
  suggestions: string[];
  next_steps: string[];
}

export interface ParameterValidationResult extends ValidationResult {
  field: string;
  value: unknown;
  rule: string;
}

export interface CodeValidationResult extends ValidationResult {
  quality_score?: number;
  syntax_errors: ValidationError[];
  logic_errors: ValidationError[];
  complexity: 'low' | 'medium' | 'high';
  metrics: {
    lines_of_code: number;
    cyclomatic_complexity: number;
    maintainability_index: number;
  };
}

export interface TemplateValidationResult extends ValidationResult {
  template_id: string;
  compatibility_score: number;
  parameter_mapping: Record<string, unknown>;
}

export interface BatchValidationResult {
  success: boolean;
  overall_score: number;
  is_ready_for_generation: boolean;
  parameter_validation?: ParameterValidationResult;
  code_validation?: CodeValidationResult;
  template_validation?: TemplateValidationResult;
  processing_time: number;
  next_steps: string[];
}

// ==================== 性能监控类型 ====================

export interface ProviderPerformance {
  provider_name: string;
  model_name: string;
  total_requests: number;
  successful_requests: number;
  failed_requests: number;
  avg_response_time: number;
  min_response_time: number;
  max_response_time: number;
  success_rate: number;
  error_rate: number;
  last_request_time: string;
  status: 'healthy' | 'degraded' | 'down';
}

export interface SystemPerformance {
  total_requests: number;
  successful_requests: number;
  failed_requests: number;
  avg_response_time: number;
  active_providers: number;
  memory_usage?: number;
  cpu_usage?: number;
  uptime: number;
  last_updated: string;
}

export interface PerformanceAlert {
  id: string;
  type: string;
  severity: 'low' | 'medium' | 'high' | 'critical';
  message: string;
  provider_name: string;
  metric_name: string;
  current_value: number;
  threshold: number;
  created_at: string;
  resolved_at?: string;
  status: 'active' | 'resolved';
}

// ==================== 文件管理类型 ====================

export interface FileInfo {
  file_id: string;
  filename: string;
  content_type: string;
  size: number;
  file_path: string;
  upload_time: string;
  user_id: string;
  category: string;
  content_hash?: string;
  description?: string;
  tags?: string[];
}

// ==================== API状态类型 ====================

export interface ApiStatus {
  isHealthy: boolean;
  lastChecked: string;
  responseTime: number;
  error?: string;
}
