package com.github.nifi.llm.model;

public class LLMResponse {
    private String content;
    private String model;
    private int inputTokens;
    private int outputTokens;

    public LLMResponse() {
    }

    public LLMResponse(String content, String model, int inputTokens, int outputTokens) {
        this.content = content;
        this.model = model;
        this.inputTokens = inputTokens;
        this.outputTokens = outputTokens;
    }

    public static LLMResponse from(String rawContent, String model) {
        return new LLMResponse(rawContent, model, 0, 0);
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public int getInputTokens() {
        return inputTokens;
    }

    public void setInputTokens(int inputTokens) {
        this.inputTokens = inputTokens;
    }

    public int getOutputTokens() {
        return outputTokens;
    }

    public void setOutputTokens(int outputTokens) {
        this.outputTokens = outputTokens;
    }
}
