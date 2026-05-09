import React, { useMemo } from 'react';
import { Card, Typography, Tag, Collapse, Space } from 'antd';
import { BulbOutlined, InfoCircleOutlined, ReadOutlined } from '@ant-design/icons';
import { useWizardV2Store } from '../store/wizardV2Store';
import { FACTORS } from '../factors/dictionary';

const { Text, Paragraph, Title } = Typography;

export const ContextAwareAssistant: React.FC<{ step: number }> = ({ step }) => {
  const { conditions, generated, workingPool, qlibParams } = useWizardV2Store();

  // 提取当前相关的因子
  const activeFactors = useMemo(() => {
    if (step !== 0) return [];

    // 简单的深度优先遍历提取条件中的因子
    const factors = new Set<string>();
    const traverse = (c: any) => {
      if (!c) return;
      if (c.factor) factors.add(c.factor);
      if (c.children) c.children.forEach(traverse);
    };
    traverse(conditions);

    return Array.from(factors).map(k => FACTORS.find(f => f.key === k)).filter(Boolean);
  }, [conditions, step]);

  const renderContent = () => {
    switch (step) {
      case 0: // 选股条件
        return (
          <Space direction="vertical" style={{ width: '100%' }}>
            <div className="bg-blue-50 p-2 rounded-xl border border-blue-100 mb-2">
              <Space align="start" size={4}>
                <BulbOutlined className="text-blue-500 mt-0.5 text-xs" />
                <Text className="text-xs leading-snug text-blue-800">
                  尝试输入组合条件，例如："低估值且高增长"，或具体指标 "PE &lt; 20 且 营收增长 &gt; 30%"。
                </Text>
              </Space>
            </div>

            {activeFactors.length > 0 ? (
              <Collapse
                ghost
                defaultActiveKey={['0']}
                size="small"
                className="custom-premium-collapse"
                items={activeFactors.map((f, i) => ({
                  key: String(i),
                  label: <Space><Tag color="blue" className="rounded-full px-2 text-[10px]">{f?.category}</Tag><span className="font-medium text-gray-700">{f?.label}</span></Space>,
                  children: (
                    <div className="bg-blue-50/30 p-3 rounded-xl border border-blue-50/50">
                      <Text type="secondary" className="text-xs leading-relaxed italic">
                        {getFactorDescription(f?.key)}
                      </Text>
                    </div>
                  ),
                }))}
              />
             ) : (
              <div className="mt-2">
                 <div className="flex items-center gap-2 mb-2">
                   <div className="h-px flex-1 bg-gray-100" />
                   <Text className="text-[10px] font-bold text-gray-400 uppercase tracking-[0.1em]">常用因子速查</Text>
                   <div className="h-px flex-1 bg-gray-100" />
                 </div>
                 
                 <div className="flex flex-wrap gap-1.5 mb-2">
                   {[
                     'market_cap', 'pe', 'pb', 'roe', 
                     'main_flow', 'pct_change', 'return_5d', 'return_20d', 
                     'turnover_rate', 'volume_ratio_5', 'volume_ma_5', 'rsi_14', 
                     'macd_hist', 'idx_hs300', 'is_st', 'limit_up_today', 'industry'
                   ].map(key => {
                     const f = FACTORS.find(item => item.key === key);
                     if (!f) return null;
                     return (
                       <button
                         key={f.key}
                         className="px-2 py-1 rounded-md text-[11px] font-medium transition-all duration-200
                           bg-gray-50 text-gray-600 border border-gray-100 hover:bg-blue-50 hover:text-blue-600 hover:border-blue-200
                           active:scale-95 shadow-sm"
                       >
                         {f.label}
                       </button>
                     );
                   })}
                 </div>

                 <div className="pt-2 border-t border-gray-50 text-center">
                   <a 
                    href="#" 
                    className="text-[10px] text-blue-500 hover:text-blue-700 font-medium transition-colors flex items-center justify-center gap-1"
                   >
                     <span>更多因子请查看帮助中心</span>
                     <ReadOutlined className="text-[10px]" />
                   </a>
                 </div>
              </div>
            )}
          </Space>
        );

      case 1: // 股票池
        return (
          <Space direction="vertical" style={{ width: '100%' }}>
            <div style={{ background: '#f6ffed', padding: 12, borderRadius: 6, border: '1px solid #b7eb8f' }}>
              <Space align="start">
                <InfoCircleOutlined style={{ color: '#52c41a', marginTop: 4 }} />
                <Text style={{ fontSize: 13 }}>
                  当前选出 <Text strong>{workingPool?.length || 0}</Text> 只股票。
                  建议检查是否存在行业过度集中风险。
                </Text>
              </Space>
            </div>
          </Space>
        );

      case 2: // 交易规则
        const isTopkDropout = (qlibParams?.strategy_type ?? 'TopkDropout') === 'TopkDropout';
        return (
          <Space direction="vertical" style={{ width: '100%' }}>
            <Card size="small" title="Qlib 参数提示" bordered={false} styles={{ body: { padding: 12 } }}>
              <Space direction="vertical" size={8} style={{ width: '100%' }}>
                {(isTopkDropout
                  ? [
                    { title: 'TopK / n_drop', desc: '建议先用 TopK=20~50、n_drop=3~10 作为稳定起点。' },
                    { title: '调仓与风控', desc: '调仓周期可先用 5 日；结合止损/止盈与仓位上限控制回撤。' }
                  ]
                  : [
                    { title: 'TopK / 权重上限', desc: '权重策略建议先用 TopK=20~50，并结合 max_weight、min_score 控制集中度。' },
                    { title: '调仓与风控', desc: '调仓周期可先用 5 日；结合止损/止盈与仓位上限控制回撤。' }
                  ]
                ).map((item) => (
                  <div key={item.title} style={{ padding: '2px 0' }}>
                    <Text style={{ fontSize: 12, fontWeight: 600 }}>{item.title}</Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: 11 }}>{item.desc}</Text>
                  </div>
                ))}
              </Space>
            </Card>
          </Space>
        );

      case 3: // 生成
        return (
           <div style={{ background: '#fff7e6', padding: 12, borderRadius: 6, border: '1px solid #ffd591' }}>
              <Space align="start">
                <ReadOutlined style={{ color: '#fa8c16', marginTop: 4 }} />
                <Text style={{ fontSize: 13 }}>
                  生成代码后，您可以：
                  1. 点击“一键回测”验证效果
                  2. 复制简报到剪贴板
                  3. 导出为 Python 文件
                </Text>
              </Space>
            </div>
        );

      default:
        return null;
    }
  };

  return (
    <Card
      title={<Space><BulbOutlined style={{ color: '#faad14' }} /><span>智能助手</span></Space>}
      size="small"
      style={{ height: 'auto', overflow: 'hidden' }}
      styles={{ body: { padding: '8px 4px' } }}
      bordered={false}
    >
      {renderContent()}
    </Card>
  );
};

// 模拟因子描述库
function getFactorDescription(key?: string) {
  const map: Record<string, string> = {
    market_cap: '市值是指一家上市公司的发行股份按市场价格计算出来的股票总价值，是衡量公司规模的重要指标。',
    pe: '市盈率 (Price-to-Earnings Ratio) 是股票价格除以每股收益的比率，用于衡量估值水平。低PE通常代表价值股。',
    roe: '净资产收益率 (Return on Equity) 衡量公司运用自有资本的效率。巴菲特非常看重的指标，通常 >15% 为优秀。',
    volume: '成交量反映了市场的活跃程度。量价配合是技术分析的核心。',
    // ... 其他可以继续补充
  };
  return map[key || ''] || '暂无详细描述，请参考通用金融定义。';
}
