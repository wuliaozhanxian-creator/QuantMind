/**
 * T2.6 渐进式 any 门禁配置
 *
 * 仅启用 @typescript-eslint/no-explicit-any 规则，作为 CI 中的渐进式门禁：
 * - 当前阶段：warning（不阻断构建，仅输出告警与计数）
 * - 后续阶段（计划 M3 末）：转为 error，并收敛告警数量上限
 *
 * 用法：npm run lint:any
 * CI 中以 advisory 模式运行（--max-warnings 不设限，仅统计）。
 */
module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2022,
    sourceType: 'module',
    ecmaFeatures: { jsx: true }
  },
  plugins: ['@typescript-eslint'],
  rules: {
    // 关键路径 any 治理：先 warning，后续转 error
    '@typescript-eslint/no-explicit-any': 'warn'
  },
  ignorePatterns: [
    'dist/**',
    'node_modules/**',
    'release/**',
    'src/**/*.test.{ts,tsx}',
    'src/**/*.spec.{ts,tsx}',
    'src/**/__tests__/**',
    'src/**/__mocks__/**'
  ]
};
