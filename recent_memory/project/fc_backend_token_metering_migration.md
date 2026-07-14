# FC后端Token计量改造记录
完成时间：2026-07-13
## 背景
metaphysics-live原有FC后端Key按调用次数扣减，无法适配不同场景下DeepSeek接口真实token消耗差异，需改为按实际token用量计量。
## 已知问题
原Python版index.py位于离线的MSI桌面设备，无法直接读取修改，基于历史Node.js版本逻辑+需求规格重新实现了Python+SQLite方案。
## 核心改动
1. SQLite keys表新增token_limit(默认500万)、tokens_used字段，保留原remaining_calls兼容旧数据，自动执行ALTER TABLE迁移
2. /verify-key返回token三字段，余额<=0直接判定Key无效
3. /analyze从DeepSeek返回的usage字段提取真实token消耗扣减，返回附带用量明细
4. /generate-key默认500万token额度，/list-keys新增token维度统计，新增/health健康检查接口
5. 安全限制：单调用超10万token直接拦截，余额不足拒绝请求
## 验证状态
本地9项全量测试全部通过，Python语法合法，兼容阿里云FC3.0多格式事件解析。
## 产出
完整部署代码路径：/app/data/所有对话/主对话/fc-backend-token-migration/index.py，配套部署文档README.md。
## 部署注意
FC运行时切换为Python3.10，入口为index.handler；默认SQLite存于/tmp目录，后续可扩展OSS同步做持久化。