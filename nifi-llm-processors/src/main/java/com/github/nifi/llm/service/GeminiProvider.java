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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class GeminiProvider implements LLMProvider {

    private final String apiKey;
    private final String model;
    private final ObjectMapper objectMapper;

    public GeminiProvider(String apiKey, String model) {
        this.apiKey = apiKey;
        this.model = model;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String complete(String systemPrompt, String userPrompt) throws IOException {
        String endpoint = "https://generativelanguage.googleapis.com/v1beta/models/"
                + URLEncoder.encode(model, StandardCharsets.UTF_8)
                + ":generateContent?key="
                + URLEncoder.encode(apiKey, StandardCharsets.UTF_8);

        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode systemInstruction = root.putObject("system_instruction");
        ArrayNode systemParts = systemInstruction.putArray("parts");
        systemParts.addObject().put("text", systemPrompt == null ? "" : systemPrompt);

        ArrayNode contents = root.putArray("contents");
        ObjectNode userMessage = contents.addObject();
        userMessage.put("role", "user");
        ArrayNode userParts = userMessage.putArray("parts");
        userParts.addObject().put("text", userPrompt == null ? "" : userPrompt);

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
                    throw new IOException("Gemini API request failed with status " + status + ": " + responseBody);
                }

                JsonNode parsed = objectMapper.readTree(responseBody);
                JsonNode textNode = parsed.path("candidates")
                        .path(0)
                        .path("content")
                        .path("parts")
                        .path(0)
                        .path("text");
                if (textNode.isMissingNode() || textNode.isNull()) {
                    throw new IOException("Invalid Gemini response: missing candidates[0].content.parts[0].text");
                }
                return textNode.asText();
            });
        }
    }
}
