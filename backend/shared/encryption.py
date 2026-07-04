"""
配置加密模块（已废弃 / DEPRECATED）
提供密码等敏感信息的加密存储功能

安全变更 (T6.4, 2026-07-04):
- 本模块已废弃，不再用于新代码。
- 原因：主密钥默认值 ``quantmind-default-key-2024``（见下文）与固定 salt
  ``quantmind_salt_v1`` 属于硬编码弱密钥，对称加密密钥可被离线推导，存在明文还原风险。
- 远程行情 PostgreSQL / Redis 凭证已统一迁移至环境变量：
    * backend/shared/market_db_manager.py   -> os.getenv("DB_PASSWORD", "")
    * backend/shared/remote_redis_client.py -> os.getenv("REMOTE_QUOTE_REDIS_PASSWORD", ...)
  两者均已不引用本模块（勘察确认无 Fernet/ConfigEncryption 依赖）。
- 保留代码仅为向后兼容历史加密数据的解密场景，新代码严禁引用。
- 计划在确认无历史加密数据依赖后整体移除本模块。
"""

import base64
import logging
import os
import warnings

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)

# 模块加载即发出 DeprecationWarning，提醒新代码不要引用本模块
warnings.warn(
    "backend.shared.encryption 已废弃 (T6.4)：主密钥与 salt 为硬编码弱默认值，"
    "新代码请直接通过环境变量注入凭证，不要引用本模块。",
    DeprecationWarning,
    stacklevel=2,
)


class ConfigEncryption:
    """配置加密类"""

    def __init__(self, master_key: str = None):
        """
        初始化加密器

        Args:
            master_key: 主密钥，如果不提供则从环境变量读取
        """
        if master_key is None:
            master_key = os.getenv("QUANTMIND_MASTER_KEY", "quantmind-default-key-2024")

        self.master_key = master_key
        self.cipher = self._create_cipher()

    def _create_cipher(self) -> Fernet:
        """创建加密器"""
        # 使用PBKDF2从主密钥派生加密密钥
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b"quantmind_salt_v1",  # 固定salt用于相同主密钥生成相同加密密钥
            iterations=100000,
            backend=default_backend(),
        )

        key = base64.urlsafe_b64encode(kdf.derive(self.master_key.encode()))
        return Fernet(key)

    def encrypt(self, plain_text: str) -> str:
        """
        加密文本

        Args:
            plain_text: 明文

        Returns:
            str: 加密后的文本（Base64编码）
        """
        try:
            if not plain_text:
                return ""

            encrypted = self.cipher.encrypt(plain_text.encode())
            return base64.urlsafe_b64encode(encrypted).decode()

        except Exception as e:
            logger.error(f"加密失败: {e}")
            raise

    def decrypt(self, encrypted_text: str) -> str:
        """
        解密文本

        Args:
            encrypted_text: 加密文本（Base64编码）

        Returns:
            str: 解密后的明文
        """
        try:
            if not encrypted_text:
                return ""

            encrypted = base64.urlsafe_b64decode(encrypted_text.encode())
            decrypted = self.cipher.decrypt(encrypted)
            return decrypted.decode()

        except Exception as e:
            logger.error(f"解密失败: {e}")
            raise

    def encrypt_dict(self, data: dict, fields: list) -> dict:
        """
        加密字典中的指定字段

        Args:
            data: 数据字典
            fields: 需要加密的字段列表

        Returns:
            dict: 加密后的字典（原字典的副本）
        """
        result = data.copy()

        for field in fields:
            if field in result and result[field]:
                result[field] = self.encrypt(str(result[field]))

        return result

    def decrypt_dict(self, data: dict, fields: list) -> dict:
        """
        解密字典中的指定字段

        Args:
            data: 数据字典
            fields: 需要解密的字段列表

        Returns:
            dict: 解密后的字典（原字典的副本）
        """
        result = data.copy()

        for field in fields:
            if field in result and result[field]:
                try:
                    result[field] = self.decrypt(str(result[field]))
                except Exception as e:
                    logger.warning(f"解密字段 {field} 失败: {e}")
                    result[field] = None

        return result


# 全局加密器实例
_global_encryptor = None


def get_encryptor() -> ConfigEncryption:
    """获取全局加密器实例"""
    global _global_encryptor
    if _global_encryptor is None:
        _global_encryptor = ConfigEncryption()
    return _global_encryptor


def encrypt_password(password: str) -> str:
    """便捷函数：加密密码"""
    return get_encryptor().encrypt(password)


def decrypt_password(encrypted_password: str) -> str:
    """便捷函数：解密密码"""
    return get_encryptor().decrypt(encrypted_password)
