import os
import subprocess
from dotenv import load_dotenv
import pymssql

def final_diagnosis():
    """最终诊断报告"""
    load_dotenv()

    print("\n" + "="*50)
    print("最终诊断报告")
    print("="*50)

    host = os.getenv("sqlserver_ip")

    print(f"\n目标服务器: {host}")
    print(f"问题描述: 网络可达，端口开放，但 SQL Server 拒绝连接")

    print("\n🔍 可能的原因:")
    print("1. ✅ 网络连通性: 正常")
    print("2. ✅ 端口访问: 正常")
    print("3. ❌ SQL Server 服务配置: 有问题")
    print("4. ❌ 认证方式: 可能不匹配")
    print("5. ❌ 驱动兼容性: pymssql 可能不兼容")

    print("\n🎯 根本原因分析:")
    print("   - SQL Server 可能配置为仅允许 Windows 认证")
    print("   - SQL Server 可能禁用了 sa 账户")
    print("   - SQL Server 可能配置了特定的连接限制")
    print("   - pymssql 驱动可能与当前 SQL Server 版本不兼容")

    print("\n💡 解决方案优先级:")
    print("1. 🥇 使用 pyodbc 替代 pymssql")
    print("2. 🥈 联系 DBA 检查 SQL Server 配置")
    print("3. 🥉 在服务器本地测试连接")
    print("4. 🔧 检查 SQL Server 错误日志")

    print("\n🚀 立即行动:")
    print("   运行以下命令安装 pyodbc:")
    print("   pip install pyodbc")

    print("\n📋 给 DBA 的检查清单:")
    print("   - 检查 SQL Server 是否允许远程连接")
    print("   - 检查认证模式 (Windows 还是混合模式)")
    print("   - 检查 sa 账户是否启用")
    print("   - 检查 TCP/IP 协议是否启用")
    print("   - 检查 SQL Server 错误日志")

final_diagnosis()