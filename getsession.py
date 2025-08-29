# import os
# import snowflake.connector
# from snowflake.snowpark import Session

# def connection() -> snowflake.connector.SnowflakeConnection:
#     """Returns a Snowflake connection object."""
#     token_path = "/snowflake/session/token"

#     if os.path.isfile(token_path):
#         creds = {
#             "host": os.getenv("SNOWFLAKE_HOST"),
#             "port": os.getenv("SNOWFLAKE_PORT"),
#             "protocol": "https",
#             "account": os.getenv("SNOWFLAKE_ACCOUNT"),
#             "authenticator": "oauth",
#             "token": open(token_path, "r").read(),
#             "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
#             "database": os.getenv("SNOWFLAKE_DATABASE"),
#             "schema": os.getenv("SNOWFLAKE_SCHEMA"),
#         }
#     else:
#         creds = {
#             "account": os.getenv("SNOWFLAKE_ACCOUNT", "kcb69289"),
#             "user": os.getenv("SNOWFLAKE_USER", "VAIDEHI"),
#             "password": os.getenv("SNOWFLAKE_PASSWORD", "Kasmodigital@123"),
#             "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
#             "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
#             "database": os.getenv("SNOWFLAKE_DATABASE", "DASH_DB"),
#             "schema": os.getenv("SNOWFLAKE_SCHEMA", "DASH_SCHEMA"),
#         }

#     return snowflake.connector.connect(**creds)

# def create_session_object() -> Session:
#     """Returns a Snowpark session object."""
#     return Session.builder.configs({"connection": connection()}).create()








import os
import snowflake.connector
from snowflake.snowpark import Session


def connection() -> snowflake.connector.SnowflakeConnection:
    """Returns a Snowflake connector connection object."""
    token_path = "/snowflake/session/token"

    if os.path.isfile(token_path):
        # OAuth token-based authentication
        creds = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "authenticator": "oauth",
            "token": open(token_path, "r").read().strip(),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }
    else:
        # Username/password-based authentication (local/dev)
        creds = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT", "kcb69289"),
            "user": os.getenv("SNOWFLAKE_USER", "VAIDEHI"),
            "password": os.getenv("SNOWFLAKE_PASSWORD", "Kasmodigital@123"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "DASH_DB"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "DASH_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        }

    return snowflake.connector.connect(**creds)


def create_session_object() -> Session:
    """
    Returns a Snowpark Session object.
    Uses dictionary credentials instead of a connector object.
    """
    token_path = "/snowflake/session/token"

    if os.path.isfile(token_path):
        creds = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "authenticator": "oauth",
            "token": open(token_path, "r").read().strip(),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }
    else:
        creds = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT", "kcb69289"),
            "user": os.getenv("SNOWFLAKE_USER", "VAIDHEI"),
            "password": os.getenv("SNOWFLAKE_PASSWORD", "Kasmodigital@123"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "DASH_DB"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "DASH_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        }

    return Session.builder.configs(creds).create()
