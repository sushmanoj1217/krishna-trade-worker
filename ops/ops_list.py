import os


def run():
return {
"env_openai_budget": os.getenv("OPENAI_BUDGET_USD", "23"),
}
