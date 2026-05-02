# 投研平台“高强度标的”显示问题技术文档

## 1. 问题描述
在投研平台概览区域，**“高强度标的”**（High-Intensity Targets）统计卡片持续显示为 `0`，即使当前推理批次中明显存在大量模型评分高于 `0.05` 的个股。

*   **UI 表现**：卡片数值为 0，下方子标签显示“模型高分命中 (≥0.05)”。
*   **预期表现**：应显示该批次中所有模型评分 `fusion_score >= 0.05` 的个股总数。

## 2. 根本原因分析

### 2.1 后端指标定义
后端 API (`/research/universe`) 返回的 `summary` 对象中包含两个关键计数指标：
1.  **`highConfidenceCount`**: 统计 `confidence_level = 'high'` 的数量。这通常是由分析师手动标注或模型置信度算法生成的标签。
2.  **`strongCount`**: 统计 `fusion_score >= 0.05` 的数量。这直接对应 UI 上定义的“高强度”口径。

### 2.2 前端代码错误
在 `ResearchPlatformPage.tsx` 中，该统计卡片的配置如下：
```tsx
<ResearchMetricCard
  icon={Flame}
  label="高强度标的"
  value={overview?.summary.highConfidenceCount || 0} // 错误点：使用了 highConfidenceCount
  subLabel="模型高分命中 (≥0.05)"
  accentColor="#f43f5e"
/>
```
**原因**：前端代码错误地将“高强度”指标绑定到了“高置信度”字段上。由于大部分自动化生成的批次中 `confidence_level` 默认为 `watch`，导致 `highConfidenceCount` 始终为 0。

## 3. 解决方案

### 3.1 修正数据绑定
修改 `ResearchPlatformPage.tsx`，将数据绑定切换为 `strongCount`：

```diff
- value={overview?.summary.highConfidenceCount || 0}
+ value={overview?.summary.strongCount || 0}
```

### 3.2 逻辑一致性校验
*   **后端过滤同步**：确认后端在计算 `strong_count` 时已经应用了 `run_id`、`tenant_id` 以及 `exclude_st`（如果用户开启）等过滤条件，确保统计数值与个股列表页的过滤逻辑同步。
*   **跨版本同步**：该修复已同步应用至 `quant` (开源版) 和 `quantmind` (社区/商业版) 的相应组件代码中。

## 4. 验证结果
1.  **数据库验证**：通过 SQL 查询 `SELECT COUNT(*) FROM qm_research_candidate_snapshot WHERE run_id = '...' AND fusion_score >= 0.05`，结果显示为 52。
2.  **UI 验证**：应用修复后，前端页面正确显示“高强度标的：52”，与数据库统计结果完全一致。

## 5. 后续建议
*   若未来需要调整“高强度”的定义阈值（如从 0.05 调整为 0.08），需同步修改后端 `research.py` 中的 `_fetch_summary` 聚合函数以及前端卡片的 `subLabel`。
