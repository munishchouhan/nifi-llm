package com.github.nifi.llm.service;

import java.io.IOException;

public interface LLMProvider {
    /**
     * Call the LLM with a system prompt and user message.
     *
     * @return raw text response
     * @throws IOException on HTTP or parse errors
     */
    String complete(String systemPrompt, String userPrompt) throws IOException;
}
