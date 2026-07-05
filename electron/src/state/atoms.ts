/**
 * 状态 atoms (T2.3 收敛后)
 *
 * @deprecated Recoil 已从项目中移除。本文件仅保留类型转发导出，
 * 以兼容既有 `import { Strategy, DashboardTab } from '../state/atoms'` 引用。
 *
 * - 类型定义已迁移到 `./types.ts`
 * - 业务状态管理统一为：Redux Toolkit (store/) + Zustand (stores/ 与 features 下的 store)
 * - 新代码请勿再从本文件导入 atom，应使用 Redux selector 或 Zustand store
 */

export type {
  DashboardTab,
  StrategyParams,
  ChatMessage,
  Strategy,
  BacktestResult,
  StrategyTemplate,
  TemplateMatch,
  ValidationError,
  ValidationResult,
  ParameterValidationResult,
  CodeValidationResult,
  TemplateValidationResult,
  BatchValidationResult,
  ProviderPerformance,
  SystemPerformance,
  PerformanceAlert,
  FileInfo,
  ApiStatus
} from './types';
