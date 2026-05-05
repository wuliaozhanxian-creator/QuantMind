import React, { useState } from 'react';
import { Row, Col, Card, Typography, Input, Button, Space, Alert, Tabs, Tag, Divider, message } from 'antd';
import { BulbOutlined, EditOutlined, ThunderboltOutlined, CheckCircleOutlined, StockOutlined } from '@ant-design/icons';
import { useWizardStore } from '../store/wizardStore';
import { parseConditions, parseText, queryPool } from '../services/wizardService';
import { SimpleLogicBuilder } from './SimpleLogicBuilder';
import { CustomStockSelector } from './CustomStockSelector';

const { Title, Text } = Typography;
const { TextArea } = Input;

export const NaturalTextInput: React.FC<{ onNext: () => void }> = ({ onNext }) => {
  const { conditions, setConditions, setPool, customPool } = useWizardStore();
  const [activeTab, setActiveTab] = useState('nlp');
  const [text, setText] = useState('');
  const [loading, setLoading] = useState(false);
  const [preview, setPreview] = useState<{ dsl?: string; mapping?: any; suggestions?: string[] }>({});
  const [matchedCount, setMatchedCount] = useState<number | null>(null);

  const useTemplate = (t: string) => {
    setText(t);
  };

  const templates = [
    { label: '全部股票', value: '全市场股票' },
    { label: '排除ST', value: '排除ST和*ST股票' },
    { label: '沪深300', value: '沪深300成分股' },
    { label: '中证1000', value: '中证1000成分股' },
    { label: '小市值', value: '总市值在10亿到100亿之间' },
    { label: '金融股', value: '金融股' },
    { label: '低估值', value: '市盈率小于20，市净率小于2' }
  ];

  const analyze = async () => {
    if (!text.trim()) {
      message.warning('请先输入选股描述');
      return;
    }
    setLoading(true);
    setMatchedCount(null);
    try {
      const parsed = await parseText(text);
      setPreview(parsed);

      if (parsed.dsl) {
        try {
          const poolRes = await queryPool({ dsl: parsed.dsl });
          if (poolRes && poolRes.items) {
            setMatchedCount(poolRes.items.length);
          }
        } catch (e) {
          console.warn('Failed to pre-calculate pool size', e);
        }
      }

      // 仅当后端提供明确 defaults 时才回填，避免使用占位值污染可视化条件。
      if (parsed.mapping?.factors && parsed.mapping?.defaults) {
        const dummyChildren = parsed.mapping.factors
          .filter((f: string) => parsed.mapping.defaults?.[f]?.threshold !== undefined)
          .map((f: string) => ({
            type: 'numeric',
            factor: f, // 假设后端返回的 factor key 与前端一致，如果不一致需要映射，这里假设 dictionary.ts 中的 key 与后端一致
            operator: '>',
            threshold: parsed.mapping.defaults[f].threshold
          }));
        if (dummyChildren.length > 0) {
          setConditions({ type: 'composite', op: 'AND', children: dummyChildren } as any);
        }
      }
      message.success('解析成功');
    } catch (err: any) {
      console.error('parseText failed', err);
      message.error(err?.message || '智能解析失败，请稍后重试');
    } finally {
      setLoading(false);
    }
  };

  const runStrategy = async () => {
    if (!preview.dsl && (!customPool || customPool.length === 0)) {
      message.warning('请先完成解析或添加自选股票');
      return;
    }
    setLoading(true);
    try {
      // 1. Get pool from DSL if available
      let poolRes: any = { items: [] };
      const dsl = preview.dsl;
      if (dsl) {
        poolRes = await queryPool({ dsl });
      }

      // 2. Merge custom pool
      if (customPool && customPool.length > 0) {
        const existingIds = new Set(poolRes.items?.map((s: any) => s.symbol));
        const customToAdd = customPool.filter(s => !existingIds.has(s.symbol));
        poolRes.items = [...(poolRes.items || []), ...customToAdd.map(s => ({
          symbol: s.symbol,
          name: s.name,
          metrics: { price: s.price || 0 } // Basic metric
        }))];
        // Recalculate summary count if needed, or just ignore for now
        if (poolRes.summary) {
          poolRes.summary.count = poolRes.items.length;
        }
      }

      if (!poolRes.items || poolRes.items.length === 0) {
        message.warning('未获取到股票池，请检查条件后重试');
        return;
      }

      setPool(poolRes);
      message.success(`已生成股票池，包含 ${poolRes.items.length} 只股票`);

      // 可视化构建器条件已经写入 store
      onNext();
    } catch (err: any) {
      console.error('runStrategy failed', err);
      message.error(err?.message || '生成股票池失败');
    } finally {
      setLoading(false);
    }
  };

  const runVisualStrategy = async () => {
    if (!conditions) {
      message.warning('请先添加筛选条件');
      return;
    }
    setLoading(true);
    try {
      const parsed = await parseConditions({ conditions });
      const poolRes = await queryPool({ dsl: parsed.dsl });
      if (!poolRes.items || poolRes.items.length === 0) {
        message.warning('未获取到股票池，请检查条件后重试');
        return;
      }
      setPool(poolRes);
      message.success(`已生成股票池，包含 ${poolRes.items.length} 只股票`);
      onNext();
    } catch (err: any) {
      console.error('runVisualStrategy failed', err);
      message.error(err?.message || '生成股票池失败');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: 1200, margin: '0 auto' }}>
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        type="card"
        items={[
          {
            key: 'nlp',
            label: <span><BulbOutlined />自然语言描述</span>,
            children: (
              <Row gutter={24} style={{ height: 600 }}>
                <Col span={16} style={{ height: '100%' }}>
                  <Card variant="borderless" style={{ boxShadow: '0 2px 8px rgba(0,0,0,0.05)', height: '100%' }}>
                    <Title level={5}>请输入选股逻辑</Title>
                    <TextArea
                      rows={8}
                      placeholder="例如：沪深300成分股中，市值大于200亿、PE小于25、PB小于3的股票"
                      value={text}
                      onChange={(e) => setText(e.target.value)}
                      style={{ fontSize: 16, padding: 12 }}
                    />
                    <div style={{ marginTop: 16 }}>
                      <Text type="secondary" style={{ marginRight: 8 }}>快速模版：</Text>
                      {templates.map(t => (
                        <Tag
                          key={t.label}
                          color="blue"
                          style={{ cursor: 'pointer', padding: '4px 8px' }}
                          onClick={() => useTemplate(t.value)}
                        >
                          {t.label}
                        </Tag>
                      ))}
                    </div>
                    <Divider />
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 16 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                        <Button type="primary" size="large" onClick={analyze} loading={loading} icon={<ThunderboltOutlined />}>
                          智能解析
                        </Button>
                        {matchedCount !== null && (
                          <span style={{ color: '#666', fontSize: 14 }}>
                            匹配到 <span style={{ color: '#1890ff', fontWeight: 600 }}>{matchedCount}</span> 只标的
                          </span>
                        )}
                      </div>
                      <Button size="large" onClick={runStrategy} disabled={!preview.dsl} icon={<CheckCircleOutlined />}>
                        确认并下一步
                      </Button>
                    </div>
                  </Card>
                </Col>
                <Col span={8} style={{ height: '100%', display: 'flex', flexDirection: 'column', gap: 16 }}>
                  <Card title="项目说明" variant="borderless" style={{ flex: 1, background: '#fafafa', overflow: 'auto' }} size="small">
                    {!preview.dsl ? (
                      <div style={{ padding: '12px 0', color: '#666', lineHeight: '1.8' }}>
                        <p style={{ textIndent: '2em', marginBottom: 16 }}>
                          作为新一代 AI 驱动量化平台，QuantMind基于Qlib框架深度定制，和传统框架不同的是Qlib颠覆了传统“手动写因子”的模式，通过深度学习自动捕捉市场规律。
                        </p>
                        <p style={{ textIndent: '2em' }}>
                          我们利用 Qlib 强大的建模能力，为您提供每日盘后自动决策服务。系统会根据当日行情实时更新模型，预测次日最具潜力的标的。您无需深谙算法，只需自定义股票池与风险偏好，剩下的交由 AI 深度模型。从海量数据中捕捉到市场规律，助您轻松把握市场先机。
                        </p>
                      </div>
                    ) : (
                      <Space direction="vertical" style={{ width: '100%' }}>
                        <Alert message="解析成功" type="success" showIcon />
                        <div>
                          <Text strong>生成的查询逻辑 (DSL):</Text>
                          <div style={{ background: '#eee', padding: 8, borderRadius: 4, marginTop: 4, fontFamily: 'monospace', fontSize: 12 }}>
                            {preview.dsl}
                          </div>
                        </div>
                        {preview.mapping?.factors && (
                          <div>
                            <Text strong>识别因子:</Text>
                            <div style={{ marginTop: 4 }}>
                              {preview.mapping.factors.map((f: string) => (
                                <Tag key={f}>{f}</Tag>
                              ))}
                            </div>
                          </div>
                        )}
                      </Space>
                    )}
                  </Card>
                </Col>
              </Row>
            )
          },
          {
            key: 'visual',
            label: <span><EditOutlined />简易构建器</span>,
            children: (
              <Card variant="borderless">
                <SimpleLogicBuilder onChange={(c) => setConditions(c)} />
                <Divider />
                <div style={{ textAlign: 'right' }}>
                  <Button type="primary" size="large" onClick={runVisualStrategy} loading={loading}>确认并下一步</Button>
                </div>
              </Card>
            )
          },
          {
            key: 'custom',
            label: <span><StockOutlined />自定义选股</span>,
            children: (
              <Card variant="borderless" style={{ height: 600 }}>
                <Alert message="手动添加您关注的特定股票，它们将与筛选结果合并。" type="info" showIcon style={{ marginBottom: 16 }} />
                <div style={{ height: 480 }}>
                  <CustomStockSelector />
                </div>
                <Divider />
                <div style={{ textAlign: 'right' }}>
                  <Button type="primary" size="large" onClick={runStrategy}>确认并下一步</Button>
                </div>
              </Card>
            )
          }
        ]}
      />
    </div>
  );
};

export default NaturalTextInput;
