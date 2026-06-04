# nifi-llm-processor

Production-quality Apache NiFi custom processor project that sends FlowFile content to LLM providers and writes responses back to the FlowFile.

## Modules

1. `nifi-llm-processors` - Processor implementation JAR
2. `nifi-llm-nar` - NiFi NAR packaging module
3. Parent `pom.xml` - Shared dependency and version management

## Build

```bash
mvn clean package
```

## Deploy

1. Copy `nifi-llm-nar/target/*.nar` to `$NIFI_HOME/extensions/`
2. Restart NiFi

## Processor Properties

| Property | Required | Default | EL Scope | Notes |
|---|---|---|---|---|
| LLM Provider | Yes | CLAUDE | None | One of CLAUDE, GEMINI, OPENAI, OLLAMA |
| API Key | No (Ollama), Yes (others) | None | Variable Registry | Sensitive |
| API Base URL | Yes | http://localhost:11434 | Variable Registry | Used by Ollama/proxies |
| Model Name | Yes | claude-3-5-sonnet-20241022 | Variable Registry | Provider-specific model |
| System Prompt | No | None | FlowFile Attributes | Static instructions |
| User Prompt Template | Yes | ${llm.input} | FlowFile Attributes | Supports NiFi EL |
| Max Tokens | Yes | 1024 | None | Integer |
| Output Mode | Yes | REPLACE_CONTENT | None | REPLACE_CONTENT, SET_ATTRIBUTE, APPEND_CONTENT |
| Output Attribute Name | Yes | llm.response | FlowFile Attributes | Used when mode is SET_ATTRIBUTE |

## Example Prompt Template

```text
Extract all email addresses from the following text:
${llm.input}
```

## Supported Providers

| Provider | Required Properties | Endpoint |
|---|---|---|
| CLAUDE | API Key, Model Name, Max Tokens | `https://api.anthropic.com/v1/messages` |
| GEMINI | API Key, Model Name | `https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent` |
| OPENAI | API Key, Model Name, Max Tokens | `https://api.openai.com/v1/chat/completions` |
| OLLAMA | Model Name (API Key not required), API Base URL | `{baseUrl}/api/chat` |

## Relationships

- `success`: FlowFile successfully processed by LLM
- `failure`: HTTP/auth/parse/general processing error
- `throttled`: Provider returned HTTP 429 rate limit
