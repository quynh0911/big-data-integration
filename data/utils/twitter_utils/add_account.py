import os
from typing import Awaitable, Callable, Dict

from dotenv import load_dotenv

from src.crawler.new_api import NewAPi

load_dotenv()


class DynamicAccountImporter:
    @staticmethod
    def create_add_account_functions(max_versions: int = 100) -> Dict[str, Callable[[], Awaitable[NewAPi]]]:
        """
        Dynamically create add_account functions for multiple versions.
        
        Args:
            max_versions (int): Maximum number of versions to search
        
        Returns:
            Dictionary of dynamically created account functions
        """
        account_functions = {}
        
        for v in range(max_versions):
            username_env_key = f"TWITTER_USER_NAME_V{v}"
            email_env_key = f"TWITTER_EMAIL_V{v}"
            
            if not (os.getenv(username_env_key) and os.getenv(email_env_key)):
                break
            
            async def create_add_account_func(version=v):
                api = NewAPi(f"accounts_pool_twitter/accounts_pool_v{version}.db")
                await api.pool.add_account(
                    os.getenv(f"TWITTER_USER_NAME_V{version}"),
                    os.getenv("TWITTER_PASSWORD_ALL"),
                    os.getenv(f"TWITTER_EMAIL_V{version}"),
                    os.getenv("TWITTER_EMAIL_PASSWORD"),
                )
                await api.pool.login_all()
                return api
        
            create_add_account_func.__name__ = f"add_account_v{v}"
            
            account_functions[f"add_account_v{v}"] = create_add_account_func
        
        return account_functions

    @classmethod
    def import_all_account_functions(cls, max_versions: int = 100):
        """
        Create a module-like object with all dynamically discovered account functions.
        
        Args:
            max_versions (int): Maximum number of versions to search
        
        Returns:
            Object with dynamically created account functions as attributes
        """
        account_module = type('DynamicAccountModule', (), {})
    
        functions = cls.create_add_account_functions(max_versions)
        
        for name, func in functions.items():
            setattr(account_module, name, func)
        
        return account_module

dynamic_account_module = DynamicAccountImporter.import_all_account_functions()