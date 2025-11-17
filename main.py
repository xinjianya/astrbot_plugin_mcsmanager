import asyncio
import time
from typing import Dict, List, Optional
import httpx
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
import astrbot.api.message_components as Comp

class InstanceCooldownManager:
    """实例操作冷却时间管理"""
    def __init__(self):
        self.cooldowns: Dict[str, float] = {}

    def check_cooldown(self, instance_id: str) -> bool:
        """检查实例是否在冷却中（10秒冷却）"""
        last_time = self.cooldowns.get(instance_id, 0)
        return time.time() - last_time < 10

    def set_cooldown(self, instance_id: str):
        """设置实例冷却时间"""
        self.cooldowns[instance_id] = time.time()

# @register(插件名, 作者, 描述, 版本, 仓库地址)
@register("MCSManager", "5060的3600马力", "MCSManager服务器管理插件(v10最终适配版)", "1.0.7")
class MCSMPlugin(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config
        self.cooldown_manager = InstanceCooldownManager()
        # 核心修改：增加超时时间，以应对跨公网连接的延迟
        self.http_client = httpx.AsyncClient(timeout=30.0)
        logger.info("MCSM插件(v10)初始化完成，超时设置为30.0s")

    async def terminate(self):
        """插件卸载时关闭HTTP客户端"""
        await self.http_client.aclose()
        logger.info("MCSM插件已卸载")

    async def make_mcsm_request(self, endpoint: str, method: str = "GET", params: dict = None, data: dict = None) -> dict:
        """发送请求到MCSManager API"""
        base_url = self.config['mcsm_url'].rstrip('/')
        
        # 确保 API 路径以 /api 开头
        if not endpoint.startswith('/api/'):
            url = f"{base_url}/api{endpoint}"
        else:
            url = f"{base_url}{endpoint}"
        
        # V10 API Key 必须放在 Query 参数中
        query_params = {"apikey": self.config["api_key"]}
        if params:
            query_params.update(params)

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Requested-With": "XMLHttpRequest"
        }

        try:
            if method.upper() == "GET":
                response = await self.http_client.get(url, params=query_params, headers=headers)
            elif method.upper() == "POST":
                response = await self.http_client.post(url, params=query_params, json=data, headers=headers)
            elif method.upper() == "PUT":
                response = await self.http_client.put(url, params=query_params, json=data, headers=headers)
            elif method.upper() == "DELETE":
                response = await self.http_client.delete(url, params=query_params, json=data, headers=headers)
            else:
                return {"status": 400, "error": "不支持的请求方法"}

            # 检查 HTTP 状态码
            if response.status_code != 200:
                # 如果返回非 200 状态码，尝试读取 JSON 错误信息
                try:
                    return response.json()
                except:
                    # 如果无法解析为 JSON，则返回状态码和文本
                    return {"status": response.status_code, "error": f"HTTP Error {response.status_code}: {response.text[:100]}..."}

            # 尝试解析 JSON 响应
            try:
                return response.json()
            except Exception as json_e:
                return {"status": 500, "error": f"JSON解析失败: {str(json_e)}"}

        # 捕获连接或超时错误
        except httpx.ConnectTimeout as e:
            return {"status": 504, "error": "连接超时 (ConnectTimeout)"}
        except httpx.ReadTimeout as e:
            return {"status": 504, "error": "读取超时 (ReadTimeout)"}
        except Exception as e:
            logger.error(f"MCSM API请求失败: {str(e)}")
            return {"status": 500, "error": str(e)}

    def is_admin_or_authorized(self, event: AstrMessageEvent) -> bool:
        """检查用户权限"""
        if event.is_admin():
            return True
        return str(event.get_sender_id()) in self.config.get("authorized_users", [])

    @filter.command("mcsm-help")
    async def mcsm_main(self, event: AstrMessageEvent):
        """显示帮助信息"""
        if not self.is_admin_or_authorized(event):
            return
            
        help_text = """
🛠️ MCSM v10 管理面板：
/mcsm-status - 面板状态概览
/mcsm-list - 节点实例列表
/mcsm-start [daemonId] [uuid] - 启动实例
/mcsm-stop [daemonId] [uuid] - 停止实例
/mcsm-cmd [daemonId] [uuid] [command] - 发送命令
/mcsm-auth [user_id] - 授权用户
/mcsm-unauth [user_id] - 取消授权
"""
        yield event.plain_result(help_text)

    @filter.command("mcsm-auth", permission_type=filter.PermissionType.ADMIN)
    async def mcsm_auth(self, event: AstrMessageEvent, user_id: str):
        """授权用户"""
        authorized_users = self.config.get("authorized_users", [])
        if user_id in authorized_users:
            yield event.plain_result(f"用户 {user_id} 已在授权列表中")
            return

        authorized_users.append(user_id)
        self.config["authorized_users"] = authorized_users
        # 假设 context.set_config 可以保存配置
        self.context.set_config(self.config)
        yield event.plain_result(f"已授权用户 {user_id}")

    @filter.command("mcsm-unauth", permission_type=filter.PermissionType.ADMIN)
    async def mcsm_unauth(self, event: AstrMessageEvent, user_id: str):
        """取消用户授权"""
        authorized_users = self.config.get("authorized_users", [])
        if user_id not in authorized_users:
            yield event.plain_result(f"用户 {user_id} 未获得授权")
            return

        authorized_users.remove(user_id)
        self.config["authorized_users"] = authorized_users
        self.context.set_config(self.config)
        yield event.plain_result(f"已取消用户 {user_id}")

    @filter.command("mcsm-list")
    async def mcsm_list(self, event: AstrMessageEvent):
        """查看实例列表 (修正 page_size)"""
        if not self.is_admin_or_authorized(event):
            yield event.plain_result("❌ 权限不足")
            return

        yield event.plain_result("正在获取节点和实例数据，请稍候...")

        # 尝试优先通过 /overview 获取节点信息
        overview_resp = await self.make_mcsm_request("/overview")
        
        nodes = []
        if overview_resp.get("status") == 200:
            nodes = overview_resp.get("data", {}).get("remote", [])
        
        if not nodes:
            yield event.plain_result(
                f"⚠️ 无法从 /overview 获取节点信息。API 响应: {overview_resp.get('error', '未知错误')}"
            )
            return

        result = "🖥️ MCSM 实例列表:\n"
        
        for node in nodes:
            node_uuid = node.get("uuid")
            node_name = node.get("remarks") or node.get("ip") or "Unnamed Node"
            
            # 获取该节点下的实例 (v10: /service/remote_service_instances)
            instances_resp = await self.make_mcsm_request(
                "/service/remote_service_instances",
                params={"daemonId": node_uuid, "page": 1, "page_size": 50}
            )

            if instances_resp.get("status") != 200:
                # 显示原始状态码和错误信息
                status_code = instances_resp.get('status', '???')
                error_detail = instances_resp.get('error', '未知API错误')
                
                if 'data' in instances_resp and isinstance(instances_resp['data'], str):
                    error_detail = instances_resp['data']

                result += f"\n❌ 节点 {node_name} (ID: {node_uuid}): 获取实例失败 (HTTP {status_code}: {error_detail})\n"
                continue

            data_block = instances_resp.get("data", {})
            # 兼容返回列表或包含 'data' 字段的对象
            instances = data_block.get("data", []) if isinstance(data_block, dict) else data_block

            if not instances:
                result += f"\n📭 节点 {node_name} (ID: {node_uuid}): 无实例\n"
                continue

            result += f"\n 节点: {node_name}\n"
            for instance in instances:
                # v10 状态码: -1:未知, 0:停止, 1:停止中, 2:启动中, 3:运行中
                status_code = instance.get("status")
                if status_code is None and "info" in instance:
                    status_code = instance["info"].get("status")
                
                status_map = {3: "🟢", 0: "🔴", 1: "🟠", 2: "🟡", -1: "⚪"}
                status_icon = status_map.get(status_code, "⚪")
                
                inst_name = instance.get("config", {}).get("nickname") or "未命名"
                inst_uuid = instance.get("instanceUuid")
                
                result += f"{status_icon} {inst_name}\n- UUID: {inst_uuid}\n"

        yield event.plain_result(result)

    @filter.command("mcsm-start")
    async def mcsm_start(self, event: AstrMessageEvent, daemon_id: str, instance_id: str):
        """启动实例"""
        if not self.is_admin_or_authorized(event):
            yield event.plain_result("❌ 权限不足")
            return

        if self.cooldown_manager.check_cooldown(instance_id):
            yield event.plain_result("⏳ 操作太快了，请稍后再试")
            return

        # v10: GET /instance/open?uuid=...&daemonId=...
        start_resp = await self.make_mcsm_request(
            "/instance/open",
            method="GET", 
            params={"uuid": instance_id, "daemonId": daemon_id}
        )
        
        if start_resp.get("status") != 200:
            err = start_resp.get("data") or start_resp.get("error") or "未知错误"
            yield event.plain_result(f"❌ 启动失败: {err}")
            return

        self.cooldown_manager.set_cooldown(instance_id)
        yield event.plain_result("✅ 启动命令已发送")

    @filter.command("mcsm-stop")
    async def mcsm_stop(self, event: AstrMessageEvent, daemon_id: str, instance_id: str):
        """停止实例"""
        if not self.is_admin_or_authorized(event):
            yield event.plain_result("❌ 权限不足")
            return

        # v10: GET /instance/stop?uuid=...&daemonId=...
        stop_resp = await self.make_mcsm_request(
            "/instance/stop",
            method="GET",
            params={"uuid": instance_id, "daemonId": daemon_id}
        )

        if stop_resp.get("status") != 200:
            err = stop_resp.get("data") or stop_resp.get("error") or "未知错误"
            yield event.plain_result(f"❌ 停止失败: {err}")
            return

        self.cooldown_manager.set_cooldown(instance_id)
        yield event.plain_result("✅ 停止命令已发送")

    @filter.command("mcsm-cmd")
    async def mcsm_cmd(self, event: AstrMessageEvent, daemon_id: str, instance_id: str, command: str):
        """发送命令"""
        if not self.is_admin_or_authorized(event):
            yield event.plain_result("❌ 权限不足")
            return

        # v10: GET /instance/command?uuid=...&daemonId=...&command=...
        cmd_resp = await self.make_mcsm_request(
            "/instance/command",
            method="GET",
            params={
                "uuid": instance_id,
                "daemonId": daemon_id,
                "command": command
            }
        )

        if cmd_resp.get("status") != 200:
            err = cmd_resp.get("data") or cmd_resp.get("error") or "未知错误"
            yield event.plain_result(f"❌ 发送失败: {err}")
            return

        # 等待一小会儿获取日志
        await asyncio.sleep(1)

        # v10: GET /instance/outputlog
        output_resp = await self.make_mcsm_request(
            "/instance/outputlog",
            method="GET",
            params={"uuid": instance_id, "daemonId": daemon_id}
        )

        output = "无返回数据"
        if output_resp.get("status") == 200:
            output = output_resp.get("data") or "无最新日志"
        
        # 截取最后几行防止刷屏
        if isinstance(output, str) and len(output) > 500:
            output = "..." + output[-500:]

        yield event.plain_result(f"✅ 命令已发送\n📝 最近日志:\n{output}")

    @filter.command("mcsm-status")
    async def mcsm_status(self, event: AstrMessageEvent):
        """查看面板状态 (精简显示 CPU/内存百分比)"""
        if not self.is_admin_or_authorized(event):
            yield event.plain_result("❌ 权限不足")
            return

        overview_resp = await self.make_mcsm_request("/overview")
        if overview_resp.get("status") != 200:
            # 捕获并显示详细错误信息
            err_msg = overview_resp.get('error', '未知连接错误，请检查配置')
            yield event.plain_result(f"❌ 获取状态失败: {err_msg}")
            return

        data = overview_resp.get("data", {})
        
        # Helper: 将秒数格式化为易读的运行时间
        def format_uptime(seconds):
            if not isinstance(seconds, (int, float)):
                return "未知"
            days = int(seconds // (3600 * 24))
            hours = int((seconds % (3600 * 24)) // 3600)
            minutes = int((seconds % 3600) // 60)
            if days > 0:
                return f"{days}天{hours}时"
            return f"{hours}时{minutes}分"
            
        # 1. 节点总览信息
        r_count = data.get("remoteCount", {})
        r_avail = r_count.get('available', 0) if isinstance(r_count, dict) else r_count
        r_total = r_count.get('total', 0) if isinstance(r_count, dict) else r_count

        total_instances = 0
        running_instances = 0
        
        # 2. 面板进程 CPU/内存/版本/运行时间
        mcsm_version = data.get("version", "未知版本")
        web_process = data.get("process", {})
        web_cpu = f"{web_process.get('cpu', 0):.2f}%"
        web_mem_bytes = web_process.get('memory', 0)
        web_mem_mb = f"{(web_mem_bytes / (1024 * 1024)):.2f}MB"
        web_uptime = format_uptime(web_process.get("uptime"))

        status_text = (
            f"📊 MCSM v{mcsm_version} 状态:\n"
            "---------------------\n"
            f"面板进程 (Web)\n"
            f"-运行时间: {web_uptime}\n"
            f"-CPU: {web_cpu}\n"
            f"-内存: {web_mem_mb}\n"
            "----------------------\n"
        )
        
        # 3. 节点系统信息
        if "remote" in data:
            for i, node in enumerate(data["remote"]):
                inst_info = node.get("instance", {})
                total_instances += inst_info.get("total", 0)
                running_instances += inst_info.get("running", 0)

                # 提取节点系统信息
                node_sys = node.get("system", {})
                node_name = node.get("remarks") or node.get("hostname") or f"Unnamed Node ({i+1})"
                
                # 提取并格式化指标
                node_cpu = f"{(node_sys.get('cpuUsage', 0) * 100):.2f}%" 
                # 内存只显示百分比
                node_mem_used_percent = f"{(node_sys.get('memUsage', 0) * 100):.2f}%" 
                
                # 磁盘信息 (只显示百分比,但是坏的？)
                disk_usage_percent = f"{(node_sys.get('diskUsage', 0) * 100):.2f}%"
                node_uptime = format_uptime(node_sys.get("uptime"))

                status_text += (
                    f"节点: {node_name}\n"
                    f"运行时间: {node_uptime}\n"
                    f"CPU: {node_cpu}\n"
                    f"内存: {node_mem_used_percent}\n"
                )

        # 4. 实例总数信息
        status_text += (
            "----------------------\n"
            f"💻 节点状态: {r_avail}/{r_total}\n"
            f"📦 实例总数: {total_instances}\n"
            f"▶️ 运行中: {running_instances}\n"
            f"提示: 使用 /mcsm-list 查看详情"
        )

        yield event.plain_result(status_text)