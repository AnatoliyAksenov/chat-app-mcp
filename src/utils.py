from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings

from gitlab import Gitlab
from gitlab.v4.objects import ProjectManager

class AppConfig(BaseSettings):
    # Required variables (no default)
    GITLAB_URL: str = Field(..., min_length=1, env="GITLAB_URL")
    GITLAB_TOKEN: str = Field(..., min_length=1, env="GITLAB_TOKEN")
    PROJECT_PATH: str = Field(..., min_length=1, env="PROJECT_PATH")

    AIRFLOW_URL: str = Field(..., min_length=1, env="AIRFLOW_URL")
    AIRFLOW_USER: str = Field(..., min_length=1, env="AIRFLOW_USER")
    AIRFLOW_PASSWORD: str = Field(..., min_length=1, env="AIRFLOW_PASSWORD")

    class Config:
        # Extra configuration
        env_file = ".env"  # Optional: load from .env file
        env_file_encoding = "utf-8"
        extra = "ignore"  # Ignore extra environment variables


_config: dict = None

def get_config():
    global _config
    if _config:
        return _config
    
    try:
        # This will automatically read from environment variables
        # or .env file
        config = AppConfig()
        return config
    
    except ValidationError as e:
        print("Configuration error:")
        print(e.errors())
        # Exit or handle error appropriately
        raise

_gitlab_project: ProjectManager = None

def get_gitlab_project():
    global _gitlab_project

    if _gitlab_project: 
        return _gitlab_project

    conf = get_config()
    __gitlab_url = conf.GITLAB_URL
    __gitlab_token = conf.GITLAB_TOKEN
    gl = Gitlab(__gitlab_url, private_token=__gitlab_token)
    project = gl.projects.get(conf.PROJECT_PATH)
    
    _gitlab_project = project

    return _gitlab_project