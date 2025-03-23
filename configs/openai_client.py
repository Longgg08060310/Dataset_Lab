from openai import AzureOpenAI


azure_client = AzureOpenAI(
    azure_endpoint = "https://openaichatgpt-me-cn.openai.azure.com/",
    api_key = "a72b7770afac45d6ba000394ddde7151",
    api_version = "2024-02-01"
)