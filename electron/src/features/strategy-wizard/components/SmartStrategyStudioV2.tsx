import React, { useState, useEffect, useRef } from 'react';
import { Layout, Steps, theme, Button, Space, Typography, Card, Divider, Breadcrumb, message, Alert, Modal, Tag } from 'antd';
import { HelpCircle } from 'lucide-react';
import {
  BulbOutlined,
  ExperimentOutlined,
  SettingOutlined,
  RocketOutlined,
  LeftOutlined,
  RightOutlined
} from '@ant-design/icons';
import { motion, AnimatePresence } from 'framer-motion';
import { NaturalTextInput } from './NaturalTextInput';
import { PoolPreview, type PoolPreviewHandle } from './PoolPreview';
import { ContextAwareAssistant } from './ContextAwareAssistant';
import QlibParamsConfig from './QlibParamsConfig';
import QlibValidatorAndSave from './QlibValidatorAndSave';
import { useWizardV2Store } from '../store/wizardV2Store';
import { PAGE_LAYOUT } from '../../../config/pageLayout';
import { generateQlib } from '../services/wizardService';
import { getWizardUserId } from '../utils/userId';
import { resolveRebalanceDays } from '../../../shared/qlib/rebalance';

const { Header, Content, Sider, Footer } = Layout;
const { Title, Text } = Typography;

const SmartStrategyStudioV2: React.FC = () => {
  const { token } = theme.useToken();
  const [currentStep, setCurrentStep] = useState(0);
  const [generating, setGenerating] = useState(false);
  const poolPreviewRef = useRef<PoolPreviewHandle>(null);

  // Use V2 store
  const {
    workingPool,
    savedPools,
    activePoolVersionId,
    conditions,
    qlibParams,
    generated,
    setGenerated,
    fetchWorkingPool,
    fetchSavedPools,
  } = useWizardV2Store();

  // Initial load
  useEffect(() => {
    fetchWorkingPool();
    fetchSavedPools();
  }, []);

  const next = async () => {
    if (currentStep === 0) {
      // Step 1 -> Step 2: Already synced to backend via setWorkingPool
    }
    
    if (currentStep === 1) {
      // Step 2 -> Step 3: PoolPreview handles activation and saving
    }

    if (currentStep === 2) {
      // Step 3 -> Step 4: Generate Strategy
      if (!activePoolVersionId) {
        message.error('未激活股票池版本，请返回第二步确认');
        return;
      }

      setGenerating(true);
      try {
        const userId = getWizardUserId();
        const res = await generateQlib({
          user_id: userId,
          conditions: conditions || {},
          pool_file_key: activePoolVersionId,
          qlib_params: {
            ...qlibParams,
            rebalance_days: resolveRebalanceDays(qlibParams),
          },
        });

        if (!res?.success || !res?.code) {
          const reason = res?.error || '生成失败';
          message.error(`生成失败: ${reason}`);
          return;
        }

        setGenerated({ code: res.code });
        message.success('策略已生成');
      } catch (e: any) {
        const reason = e?.response?.data?.error || e?.message || '生成失败';
        message.error(`生成失败: ${reason}`);
        return;
      } finally {
        setGenerating(false);
      }
    }
    
    setCurrentStep(Math.min(currentStep + 1, 3));
  };

  const prev = () => setCurrentStep(Math.max(currentStep - 1, 0));

  const steps = [
    {
      title: '条件选股',
      icon: <BulbOutlined />,
      description: '自然语言或可视化构建',
      content: <NaturalTextInput onNext={next} />
    },
    {
      title: '确定股票池',
      icon: <ExperimentOutlined />,
      description: '预览与验证股票池',
      content: <PoolPreview ref={poolPreviewRef} onNext={next} onBack={prev} />
    },
    {
      title: '策略参数',
      icon: <SettingOutlined />,
      description: 'Qlib专用参数（TopK / 调仓 / 风控）',
      content: <QlibParamsConfig onNext={next} onBack={prev} />
    },
    {
      title: '验证与保存',
      icon: <RocketOutlined />,
      description: 'Qlib验证并保存策略',
      content: <QlibValidatorAndSave onBack={prev} />
    }
  ];

  const canProceed = (stepIndex: number) => {
    if (stepIndex === 0) {
      return workingPool.length > 0;
    }
    if (stepIndex === 1) {
      return workingPool.length > 0;
    }
    return true;
  };

  const nextDisabled = !canProceed(currentStep);

  return (
    <div className={PAGE_LAYOUT.outerClass}>
      <div className={PAGE_LAYOUT.frameClass}>
      <Modal
        title={
          <div style={{ textAlign: 'center', fontWeight: 800, letterSpacing: 0.3 }}>
            正在生成策略
          </div>
        }
        open={generating}
        closable={false}
        maskClosable={false}
        footer={null}
        centered
        styles={{
          header: { borderBottom: 'none', paddingBottom: 8 },
          body: { paddingTop: 6 }
        }}
      >
        <div
          style={{
            borderRadius: 12,
            padding: 16,
            background:
              'radial-gradient(1200px 220px at 50% -40px, rgba(59,130,246,0.18), transparent 55%),' +
              'linear-gradient(180deg, rgba(15,23,42,0.02), rgba(15,23,42,0.00))',
            border: '1px solid rgba(148,163,184,0.35)'
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'center', padding: '10px 0 14px' }}>
            <div
              style={{
                width: 220,
                height: 64,
                display: 'grid',
                gridTemplateColumns: 'repeat(7, 1fr)',
                columnGap: 8,
                alignItems: 'end'
              }}
              aria-label="策略生成进度动画"
            >
              {Array.from({ length: 7 }).map((_, i) => (
                <motion.div
                  key={i}
                  initial={{ scaleY: 0.35, opacity: 0.65 }}
                  animate={{
                    scaleY: [0.35, 1.05, 0.5, 0.95, 0.35],
                    opacity: [0.55, 1, 0.7, 1, 0.55]
                  }}
                  transition={{
                    duration: 1.35,
                    repeat: Infinity,
                    ease: 'easeInOut',
                    delay: i * 0.08
                  }}
                  style={{
                    height: 52,
                    transformOrigin: 'bottom',
                    borderRadius: 10,
                    background:
                      'linear-gradient(180deg, rgba(59,130,246,0.95), rgba(14,165,233,0.35))',
                    boxShadow: '0 10px 26px rgba(59,130,246,0.20)'
                  }}
                />
              ))}
            </div>
          </div>

          <Text style={{ color: '#475569', lineHeight: 1.7 }}>
            当前大模型正在根据您的需求生成策略，请稍等，整个过程可能需要 1-2 分钟。
            当策略生成并解析完毕，将自动关闭该窗口并进入下一步。
          </Text>
        </div>
      </Modal>
      <Layout
        style={{
          width: '100%',
          height: '100%',
          background: '#fff',
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column'
        }}
      >
        {/* 顶部工具栏 - 对齐回测中心 */}
        <Header className={PAGE_LAYOUT.headerClass} style={{ height: PAGE_LAYOUT.headerHeight, padding: '0 24px', zIndex: 100 }}>
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-purple-500 rounded-xl flex items-center justify-center shadow-md">
              <span className="text-white font-bold text-base">S</span>
            </div>
            <Title level={4} style={{ margin: 0, background: 'linear-gradient(45deg, #3b82f6, #8b5cf6)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', fontWeight: 700 }}>
              Smart Studio
            </Title>
          </div>
          <div style={{ flex: 1 }} />
          <Steps
            current={currentStep}
            size="small"
            style={{ width: 500 }}
            items={steps.map(s => ({ title: s.title }))}
          />
        </Header>

        <Layout style={{ flex: 1, overflow: 'hidden' }}>
          {/* 左侧导航栏 - 模拟 Sidebar */}
          <Sider
            width={280}
            theme="light"
            style={{
              borderRight: '1px solid #e2e8f0',
              background: '#fff'
            }}
          >
            <div className="flex flex-col h-full">
              <div style={{ flex: 1, overflow: 'auto', padding: '24px 12px 12px' }}>
                <Steps
                  direction="vertical"
                  current={currentStep}
                  items={steps.map(s => ({ title: s.title, icon: s.icon, description: s.description }))}
                  onChange={(i) => setCurrentStep(i)}
                  size="small"
                  className="custom-steps"
                />
                <Divider style={{ margin: '12px 0' }} />
                <div style={{ padding: '0 4px 24px' }}>
                  <ContextAwareAssistant step={currentStep} />
                  {currentStep === 4 && (
                    <Alert
                      type="info"
                      showIcon
                      className="mt-3"
                      message="当前只执行语法检测，如需评估量化效果，请前往回测中心模块"
                    />
                  )}
                </div>
              </div>

              {/* 底部帮助中心链接 - 标准化样式 */}
              <div className="border-t border-gray-200 p-4 shrink-0 mt-auto">
                <a
                  href="https://www.quantmindai.cn/help"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="w-full flex items-center gap-3 px-4 py-3 rounded-2xl text-gray-600 hover:text-blue-600 hover:bg-blue-50 transition-colors"
                >
                  <HelpCircle className="w-5 h-5" />
                  <span className="text-sm">帮助文档</span>
                </a>
              </div>
            </div>
          </Sider>

          <Layout style={{ background: '#f8fafc', padding: '0', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            {/* 面包屑/标题栏 - 增加呼吸感 */}
            <div className="px-6 py-4 flex items-center justify-between bg-white border-b border-gray-200">
              <div className="flex flex-col">
                <Breadcrumb 
                  items={[
                    { title: '策略开发' },
                    { title: <Text strong>智能策略向导</Text> }
                  ]} 
                  style={{ marginBottom: 4, fontSize: 12 }}
                />
                <Title level={3} style={{ margin: 0, fontSize: '1.25rem', fontWeight: 600, color: '#1e293b' }}>
                  {steps[currentStep].title}
                </Title>
              </div>
              <Space size="middle">
                <Button
                  onClick={prev}
                  disabled={currentStep === 0}
                  icon={<LeftOutlined />}
                  className="rounded-xl border-gray-200 hover:text-blue-500"
                >
                  上一步
                </Button>
                {currentStep < steps.length - 1 && (
                  <Button
                    type="primary"
                    onClick={() => {
                      if (currentStep === 1) {
                        // 第二步必须走 PoolPreview 的保存拦截逻辑，避免误跳第三步
                        poolPreviewRef.current?.triggerSaveAndNext();
                        return;
                      }
                      next();
                    }}
                    icon={currentStep === 2 ? <RocketOutlined /> : <RightOutlined />}
                    disabled={nextDisabled || generating}
                    loading={generating}
                    className="rounded-xl bg-gradient-to-r from-blue-500 to-purple-500 border-none shadow-md hover:shadow-lg transition-all"
                  >
                    {currentStep === 2 ? '立即生成策略' : '下一步'}
                  </Button>
                )}
              </Space>
            </div>

            {/* 核心卡片容器 - 严格对齐回测中心 */}
            <Content
              style={{
                padding: '24px',
                overflowY: 'auto',
                position: 'relative',
                flex: 1
              }}
            >
              <AnimatePresence mode="wait">
                <motion.div
                  key={currentStep}
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -10 }}
                  transition={{ duration: 0.2 }}
                  className="bg-white overflow-hidden"
                  style={{
                    padding: 24,
                    borderRadius: '32px',
                    minHeight: 'calc(100% - 20px)',
                    display: 'flex',
                    flexDirection: 'column'
                  }}
                >
                  <div className="flex-1">
                    {steps[currentStep].content}
                  </div>
                </motion.div>
              </AnimatePresence>
            </Content>

            {/* 状态栏 */}
            <Footer style={{
              display: 'flex',
              justifyContent: 'flex-end',
              alignItems: 'center',
              padding: '12px 24px',
              background: 'rgba(255, 255, 255, 0.8)',
              backdropFilter: 'blur(4px)',
              borderTop: '1px solid #e2e8f0',
              zIndex: 10
            }}>
              <Space>
                <div className="inline-block h-4 w-px bg-gray-300 mx-2" />
                <div className="flex items-center gap-2">
                  <div className={`w-2 h-2 rounded-full ${generated?.code ? 'bg-green-500' : 'bg-amber-500 animate-pulse'}`} />
                  <Text type="secondary" className="text-xs">状态: {generated?.code ? '已生成' : '草稿'}</Text>
                </div>
                <Text type="secondary" className="text-xs">
                  股票池: <span className="font-medium text-gray-700">{workingPool ? `${workingPool.length} 只` : '未计算'}</span>
                </Text>
                <Text type="secondary" className="text-xs">
                  最后更新: <span className="font-mono">{new Date().toLocaleTimeString()}</span>
                </Text>
              </Space>
            </Footer>
          </Layout>
        </Layout>
      </Layout>
      </div>
    </div>
  );
};

export default SmartStrategyStudioV2;
