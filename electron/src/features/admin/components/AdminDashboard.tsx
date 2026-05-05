import React, { useEffect, useState } from 'react';
import { Card, Row, Col, Statistic, Spin, message, Result, Button, Space, Typography, Tag, Progress, List, Badge, Divider } from 'antd';
import { 
    UserOutlined, 
    LineChartOutlined, 
    MessageOutlined, 
    HeartOutlined, 
    LoginOutlined, 
    HomeOutlined,
    ThunderboltOutlined,
    DeploymentUnitOutlined,
    DatabaseOutlined,
    GlobalOutlined,
    ApiOutlined,
    SwapOutlined,
    CheckCircleFilled,
    ClockCircleOutlined,
    AreaChartOutlined
} from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';
import axios from 'axios';
import { adminService } from '../services/adminService';
import { authService } from '../../auth/services/authService';
import { useAppDispatch } from '../../../store';
import { logout } from '../../auth/store/authSlice';
import { DashboardMetrics } from '../types';

const { Title, Text } = Typography;

export const AdminDashboard: React.FC = () => {
    const dispatch = useAppDispatch();
    const navigate = useNavigate();
    const location = useLocation();
    const [metrics, setMetrics] = useState<DashboardMetrics | null>(null);
    const [loading, setLoading] = useState(true);
    const [authError, setAuthError] = useState<{ status: number; message: string } | null>(null);

    useEffect(() => {
        loadMetrics();
    }, []);

    const loadMetrics = async () => {
        try {
            adminService.clearMetricsUnauthorized();
            setAuthError(null);
            const data = await adminService.getMetrics();
            setMetrics(data);
        } catch (err: any) {
            const status = err?.response?.status;
            const isLocked = String(err?.message || '').includes('ADMIN_METRICS_UNAUTHORIZED_LOCKED');
            const isAuthError = isLocked || status === 401 || status === 403 || (axios.isAxiosError(err) && (err.response?.status === 401 || err.response?.status === 403));
            
            if (isAuthError) {
                adminService.markMetricsUnauthorized();
                setAuthError({
                    status: status || 401,
                    message: status === 403 ? '您没有访问管理面板的权限。' : '您的登录会话已过期，请重新登录。'
                });
                return;
            }
            message.error('加载系统指标失败');
        } finally {
            setLoading(false);
        }
    };

    if (authError) {
        return (
            <div className="flex items-center justify-center py-20 bg-white border border-slate-200 rounded-3xl shadow-sm">
                <Result
                    status="403"
                    title={<span className="text-xl font-bold text-slate-800">访问受限</span>}
                    subTitle={<span className="text-slate-500">{authError.message}</span>}
                    extra={[
                        <Button 
                            type="primary" 
                            key="login" 
                            icon={<LoginOutlined />}
                            size="large"
                            className="h-11 rounded-xl px-8 bg-slate-900 border-none shadow-sm"
                            onClick={async () => {
                                await dispatch(logout());
                                navigate('/auth/login', { state: { from: location } });
                            }}
                        >
                            重新登录
                        </Button>,
                        <Button 
                            key="home" 
                            icon={<HomeOutlined />}
                            size="large"
                            className="h-11 rounded-xl px-8 text-slate-600 font-bold hover:bg-slate-50 transition-all border-slate-200"
                            onClick={() => navigate('/')}
                        >
                            返回首页
                        </Button>
                    ]}
                />
            </div>
        );
    }

    if (loading || !metrics) return (
        <div className="w-full flex flex-col items-center justify-center py-32 space-y-4">
            <Spin size="large" />
            <Text className="text-slate-400 font-bold text-xs">正在加载指标数据...</Text>
        </div>
    );

    const serviceStats = [
        { name: 'API 网关', port: '8000', desc: '认证与统一网关', icon: <ApiOutlined />, load: 42, color: 'blue' },
        { name: '推理引擎', port: '8001', desc: 'AI 推理与回测', icon: <ThunderboltOutlined />, load: 84, color: 'orange' },
        { name: '交易核心', port: '8002', desc: '订单与持仓管理', icon: <SwapOutlined />, load: 18, color: 'emerald' },
        { name: '实时行情', port: '8003', desc: '行情接入与推送', icon: <GlobalOutlined />, load: 31, color: 'indigo' },
    ];

    return (
        <div className="space-y-8 animate-in fade-in duration-500">
            {/* Header */}
            <div className="flex items-center justify-between mb-2">
                <div>
                    <Title level={4} className="!m-0 !font-black !text-slate-800 text-lg">系统控制台</Title>
                    <Text className="text-slate-400 text-xs font-medium">基础设施节点监控与管理</Text>
                </div>
                <Button 
                    icon={<ThunderboltOutlined />} 
                    onClick={loadMetrics}
                    className="rounded-xl font-bold bg-white text-slate-800 border-slate-200 hover:border-slate-800 hover:text-slate-800 shadow-sm h-10 px-6"
                >
                    刷新数据
                </Button>
            </div>

            {/* Core Services Grid */}
            <Row gutter={[20, 20]}>
                {serviceStats.map((s) => (
                    <Col xs={24} sm={12} lg={6} key={s.name}>
                        <Card className="rounded-2xl border-slate-200 shadow-sm hover:shadow-md transition-all">
                            <div className="flex items-center justify-between mb-4">
                                <div className="flex items-center gap-3">
                                    <div className={`w-10 h-10 rounded-xl bg-slate-50 flex items-center justify-center text-slate-600 border border-slate-100`}>
                                        {s.icon}
                                    </div>
                                    <div>
                                        <div className="flex items-center gap-1.5">
                                            <Text className="font-black text-slate-800 text-sm">{s.name}</Text>
                                            <Badge status="processing" color="#10b981" />
                                        </div>
                                        <Text className="text-[10px] text-slate-400 font-bold">端口 {s.port}</Text>
                                    </div>
                                </div>
                                <Tag color="success" className="m-0 border-none rounded-full px-2 text-[9px] font-black bg-emerald-50 text-emerald-600">运行中</Tag>
                            </div>
                            <div className="space-y-1.5">
                                <div className="flex justify-between items-center text-[10px] font-black mb-1">
                                    <span className="text-slate-400">负载系数</span>
                                    <span className={s.load > 80 ? "text-rose-500" : "text-slate-800"}>{s.load}%</span>
                                </div>
                                <div className="h-1.5 w-full bg-slate-100 rounded-full overflow-hidden">
                                    <div 
                                        className={`h-full rounded-full transition-all duration-1000 ${s.load > 80 ? 'bg-rose-500' : 'bg-slate-800'}`} 
                                        style={{ width: `${s.load}%` }} 
                                    />
                                </div>
                                <Text className="text-[10px] text-slate-400 font-medium block pt-1">{s.desc}</Text>
                            </div>
                        </Card>
                    </Col>
                ))}
            </Row>

            <Divider className="!m-0 border-slate-100" />

            <Row gutter={[24, 24]}>
                {/* Main Stats */}
                <Col span={24} lg={16}>
                    <div className="space-y-6">
                        <Title level={5} className="!m-0 !font-black !text-slate-800 text-xs opacity-50">全局统计</Title>
                        <Row gutter={[20, 20]}>
                            {[
                                { title: "总用户数", value: metrics.users.total, sub: `今日新增 ${metrics.users.new_today} 人`, icon: <UserOutlined /> },
                                { title: "实盘策略", value: metrics.strategies.live, sub: `共 ${metrics.strategies.total} 个策略`, icon: <LineChartOutlined /> },
                                { title: "数据记录", value: metrics.content.posts, sub: "社区互动数据", icon: <DatabaseOutlined /> },
                                { title: "系统运行", value: metrics.system.uptime_days, suffix: "天", sub: `健康度: ${metrics.system.health_score}%`, icon: <HeartOutlined /> }
                            ].map((item, idx) => (
                                <Col span={12} key={idx}>
                                    <Card className="rounded-2xl border-slate-100 bg-white shadow-sm">
                                        <Statistic 
                                            title={<span className="text-[10px] font-black text-slate-400">{item.title}</span>}
                                            value={item.value}
                                            suffix={item.suffix}
                                            valueStyle={{ fontWeight: 900, color: '#1e293b', fontSize: '24px', letterSpacing: '-0.025em' }}
                                            prefix={<div className="text-slate-300 mr-2">{item.icon}</div>}
                                        />
                                        <div className="mt-2 text-[11px] font-bold text-slate-400 flex items-center gap-1">
                                            <div className="w-1 h-1 rounded-full bg-slate-200" />
                                            {item.sub}
                                        </div>
                                    </Card>
                                </Col>
                            ))}
                        </Row>
                        
                        <Card className="rounded-2xl border-slate-100 shadow-sm" title={<span className="text-xs font-black text-slate-500">节点性能历史</span>}>
                            <div className="py-12 flex flex-col items-center justify-center bg-slate-50 rounded-xl border border-dashed border-slate-200">
                                <AreaChartOutlined className="text-slate-300 text-3xl mb-3" />
                                <Text className="text-slate-400 font-bold text-xs">实时吞吐量数据收集中...</Text>
                            </div>
                        </Card>
                    </div>
                </Col>

                {/* Side Activity */}
                <Col span={24} lg={8}>
                    <div className="space-y-6">
                        <Title level={5} className="!m-0 !font-black !text-slate-800 text-xs opacity-50">最近事件</Title>
                        <Card className="rounded-2xl border-slate-200 shadow-sm p-2">
                            <List
                                itemLayout="horizontal"
                                dataSource={[
                                    { title: '新 API 节点加入集群', time: '2分钟前', type: 'success' },
                                    { title: '策略执行完成', time: '14分钟前', type: 'info' },
                                    { title: '市场数据同步开始', time: '45分钟前', type: 'info' },
                                    { title: '引擎服务内存使用率偏高', time: '1小时前', type: 'warning' },
                                    { title: '系统备份完成', time: '3小时前', type: 'success' },
                                    { title: '新用户注册', time: '5小时前', type: 'info' },
                                ]}
                                renderItem={(item) => (
                                    <List.Item className="!px-4 !py-3 hover:bg-slate-50 rounded-xl transition-all cursor-pointer">
                                        <List.Item.Meta
                                            avatar={
                                                <div className={`mt-1.5 w-2 h-2 rounded-full ${
                                                    item.type === 'success' ? 'bg-emerald-500' : 
                                                    item.type === 'warning' ? 'bg-rose-500' : 'bg-blue-500'
                                                }`} />
                                            }
                                            title={<span className="text-xs font-bold text-slate-700">{item.title}</span>}
                                            description={<span className="text-[10px] text-slate-400 font-bold">{item.time}</span>}
                                        />
                                    </List.Item>
                                )}
                            />
                            <div className="p-4 pt-2">
                                <Button block className="rounded-xl border-slate-200 text-slate-500 font-bold text-xs h-10 hover:border-slate-800 hover:text-slate-800">
                                    查看审计日志
                                </Button>
                            </div>
                        </Card>

                        <div className="bg-slate-900 rounded-3xl p-6 text-white overflow-hidden relative shadow-lg shadow-slate-200">
                             <div className="relative z-10">
                                <div className="flex items-center gap-2 mb-4">
                                    <div className="w-8 h-8 rounded-lg bg-white/10 flex items-center justify-center text-white">
                                        <DeploymentUnitOutlined />
                                    </div>
                                    <Text className="text-white text-xs font-black">网络状态</Text>
                                </div>
                                <div className="flex items-baseline gap-1 mb-1">
                                    <span className="text-3xl font-black tracking-tighter">0.42</span>
                                    <span className="text-white/40 font-bold text-xs">ms</span>
                                </div>
                                <Text className="text-white/40 text-[10px] font-bold block mb-4">平均 API 延迟</Text>
                                <Progress percent={98} size="small" strokeColor="#10b981" trailColor="rgba(255,255,255,0.1)" strokeWidth={4} />
                                <Text className="text-emerald-400 text-[10px] font-black block mt-2">可靠性极佳</Text>
                             </div>
                             <div className="absolute -bottom-6 -right-6 w-32 h-32 bg-white/5 rounded-full blur-3xl" />
                        </div>
                    </div>
                </Col>
            </Row>
        </div>
    );
};
