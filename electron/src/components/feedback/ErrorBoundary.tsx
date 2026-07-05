/**
 * 错误边界（T2.4 合并后转发）
 *
 * 历史上的富功能实现已合并到 `components/common/ErrorBoundary.tsx`，
 * 本文件仅做转发导出，保持既有 `from '@/components/feedback/ErrorBoundary'`
 * 引用路径可用，避免重复维护两套实现。
 */

export {
  ErrorBoundary,
  SimpleErrorBoundary,
  withErrorBoundary,
  default
} from '../common/ErrorBoundary';

export type { ErrorBoundaryProps } from '../common/ErrorBoundary';
