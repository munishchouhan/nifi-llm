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

public class OllamaProvider implements LLMProvider {

    private final String baseUrl;
    private final String model;
    private final ObjectMapper objectMapper;

    public OllamaProvider(String baseUrl, String model) {
        this.baseUrl = baseUrl;
        this.model = model;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String complete(String systemPrompt, String userPrompt) throws IOException {
        String endpoint = normalizeBaseUrl(baseUrl) + "/api/chat";

        ObjectNode root = objectMapper.createObjectNode();
        root.put("model", model);
        root.put("stream", false);

        ArrayNode messages = root.putArray("messages");
        ObjectNode systemMessage = messages.addObject();
        systemMessage.put("role", "system");
        systemMessage.put("content", systemPrompt == null ? "" : systemPrompt);

        ObjectNode userMessage = messages.addObject();
        userMessage.put("role", "user");
        userMessage.put("content", userPrompt == null ? "" : userPrompt);

        HttpPost request = new HttpPost(endpoint);
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
                    throw new IOException("Ollama API request failed with status " + status + ": " + responseBody);
                }

                JsonNode parsed = objectMapper.readTree(responseBody);
                JsonNode textNode = parsed.path("message").path("content");
                if (textNode.isMissingNode() || textNode.isNull()) {
                    throw new IOException("Invalid Ollama response: missing message.content");
                }
                return textNode.asText();
            });
        }
    }

    private String normalizeBaseUrl(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "http://localhost:11434";
        }
        String normalized = value.trim();
        if (normalized.endsWith("/")) {
            return normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
