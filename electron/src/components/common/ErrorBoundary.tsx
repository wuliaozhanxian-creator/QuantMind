import React, { Component, ErrorInfo, ReactNode } from 'react';

/**
 * 统一错误边界 (T2.4 合并)
 *
 * 历史上存在两套实现：
 * - `components/common/ErrorBoundary.tsx`：简单版（默认 UI + DEV 详情）
 * - `components/feedback/ErrorBoundary.tsx`：富功能版（onError / showDetails / withErrorBoundary HOC / logErrorToService）
 *
 * T2.4 已将两者合并为本单一实现，`components/feedback/ErrorBoundary.tsx`
 * 改为转发导出，避免重复维护。
 */

export interface ErrorBoundaryProps {
  children: ReactNode;
  fallback?: ReactNode;
  /** 错误捕获时的外部回调（可用于上报日志服务） */
  onError?: (error: Error, errorInfo: ErrorInfo) => void;
  /** 是否展示错误堆栈详情（默认仅 DEV 模式展示） */
  showDetails?: boolean;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
  errorInfo: ErrorInfo | null;
}

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null
    };
  }

  static getDerivedStateFromError(error: Error): Partial<ErrorBoundaryState> {
    return {
      hasError: true,
      error
    };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    console.error('ErrorBoundary caught an error:', error, errorInfo);

    this.setState({
      error,
      errorInfo
    });

    // 调用外部错误处理函数
    if (this.props.onError) {
      this.props.onError(error, errorInfo);
    }

    this.logErrorToService(error, errorInfo);
  }

  /**
   * 错误日志上报（预留远程日志接入点）
   */
  logErrorToService(error: Error, errorInfo: ErrorInfo): void {
    const errorLog = {
      message: error.message,
      stack: error.stack,
      componentStack: errorInfo.componentStack,
      timestamp: new Date().toISOString()
    };

    console.log('Error logged:', errorLog);
    // TODO: 接入远程日志服务（Sentry / LogRocket 等）
  }

  handleReset = (): void => {
    this.setState({
      hasError: false,
      error: null,
      errorInfo: null
    });
  };

  handleReload = (): void => {
    window.location.reload();
  };

  render(): ReactNode {
    const { hasError, error, errorInfo } = this.state;
    const { children, fallback, showDetails } = this.props;

    if (!hasError) {
      return children;
    }

    // 自定义 fallback 优先
    if (fallback) {
      return fallback;
    }

    // 默认错误 UI
    const shouldShowDetails = showDetails ?? import.meta.env.DEV;

    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="max-w-md w-full bg-white rounded-lg shadow-lg p-6">
          <div className="text-center">
            <div className="text-6xl mb-4">⚠️</div>
            <h2 className="text-2xl font-bold text-gray-800 mb-2">出现错误</h2>
            <p className="text-gray-600 mb-6">
              {error?.message || '应用遇到了一个意外错误，请尝试刷新页面。'}
            </p>

            <div className="space-y-3">
              <button
                onClick={this.handleReset}
                className="w-full px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
              >
                重试
              </button>

              <button
                onClick={this.handleReload}
                className="w-full px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 transition-colors"
              >
                刷新页面
              </button>
            </div>

            {/* 错误详情（showDetails 显式开启，或 DEV 模式默认开启） */}
            {shouldShowDetails && error && (
              <details className="mt-6 text-left">
                <summary className="cursor-pointer text-sm text-gray-500 hover:text-gray-700">
                  错误详情
                </summary>
                <div className="mt-2 p-3 bg-gray-100 rounded text-xs text-gray-700 overflow-auto">
                  <pre>{error.toString()}</pre>
                  {error.stack && <pre className="mt-2">{error.stack}</pre>}
                  {errorInfo && (
                    <pre className="mt-2">{errorInfo.componentStack}</pre>
                  )}
                </div>
              </details>
            )}
          </div>
        </div>
      </div>
    );
  }
}

/**
 * 轻量级错误边界（用于包装单个组件）
 */
interface SimpleErrorBoundaryProps {
  children: ReactNode;
  fallback?: ReactNode;
  onError?: (error: Error, errorInfo: ErrorInfo) => void;
}

export const SimpleErrorBoundary: React.FC<SimpleErrorBoundaryProps> = ({
  children,
  fallback,
  onError
}) => {
  return (
    <ErrorBoundary
      onError={onError}
      fallback={fallback || (
        <div className="p-4 text-center">
          <div className="text-red-500 text-sm">组件加载失败</div>
          <button
            onClick={() => window.location.reload()}
            className="mt-2 text-xs text-blue-600 hover:text-blue-800"
          >
            刷新重试
          </button>
        </div>
      )}
    >
      {children}
    </ErrorBoundary>
  );
};

/**
 * 高阶组件：为目标组件包裹错误边界
 */
export const withErrorBoundary = <P extends object>(
  ComponentToWrap: React.ComponentType<P>,
  errorBoundaryProps?: Omit<ErrorBoundaryProps, 'children'>
) => {
  const WrappedComponent: React.FC<P> = (props) => {
    return (
      <ErrorBoundary {...errorBoundaryProps}>
        <ComponentToWrap {...props} />
      </ErrorBoundary>
    );
  };

  WrappedComponent.displayName = `withErrorBoundary(${ComponentToWrap.displayName || ComponentToWrap.name || 'Component'})`;

  return WrappedComponent;
};

export default ErrorBoundary;
