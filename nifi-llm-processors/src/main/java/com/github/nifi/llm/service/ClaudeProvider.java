package com.github.nifi.llm.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ClaudeProvider implements LLMProvider {

    private static final String API_URL = "https://api.anthropic.com/v1/messages";

    private final String apiKey;
    private final String model;
    private final int maxTokens;
    private final ObjectMapper objectMapper;

    public ClaudeProvider(String apiKey, String model, int maxTokens) {
        this.apiKey = apiKey;
        this.model = model;
        this.maxTokens = maxTokens;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String complete(String systemPrompt, String userPrompt) throws IOException {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("model", model);
        root.put("max_tokens", maxTokens);
        root.put("system", systemPrompt == null ? "" : systemPrompt);

        ArrayNode messages = root.putArray("messages");
        ObjectNode userMessage = messages.addObject();
        userMessage.put("role", "user");
        userMessage.put("content", userPrompt == null ? "" : userPrompt);

        HttpPost request = new HttpPost(API_URL);
        request.setHeader("x-api-key", apiKey);
        request.setHeader("anthropic-version", "2023-06-01");
        request.setHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity(objectMapper.writeValueAsString(root), ContentType.APPLICATION_JSON));

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            return client.execute(request, response -> {
                int status = response.getCode();
                HttpEntity entity = response.getEntity();
                String responseBody = entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);

                if (status == 429) {
                    throw new RateLimitException("rate_limited");
                }
                if (status < 200 || status >= 300) {
                    throw new IOException("Claude API request failed with status " + status + ": " + responseBody);
                }

                JsonNode parsed = objectMapper.readTree(responseBody);
                JsonNode contentNode = parsed.path("content");
                if (!contentNode.isArray() || contentNode.isEmpty()) {
                    throw new IOException("Invalid Claude response: missing content array");
                }
                JsonNode textNode = contentNode.get(0).path("text");
                if (textNode.isMissingNode() || textNode.isNull()) {
                    throw new IOException("Invalid Claude response: missing content[0].text");
                }
                return textNode.asText();
            });
        }
    }
}
