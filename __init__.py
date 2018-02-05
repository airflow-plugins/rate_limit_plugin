from airflow.plugins_manager import AirflowPlugin
from RateLimitPlugin.operators.rate_limit_operator import RateLimitOperator


class RateLimitPlugin(AirflowPlugin):
    name = "RateLimitPlugin"
    operators = [RateLimitOperator]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
