package com.github.nifi.llm.processor;

import com.github.nifi.llm.service.LLMProvider;
import com.github.nifi.llm.service.RateLimitException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InvokeLLMProcessorTest {

    private TestRunner runner;
    private TestableInvokeLLMProcessor processor;

    @Before
    public void setup() {
        processor = new TestableInvokeLLMProcessor();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(InvokeLLMProcessor.LLM_PROVIDER, "CLAUDE");
        runner.setProperty(InvokeLLMProcessor.API_KEY, "test-key");
        runner.setProperty(InvokeLLMProcessor.MODEL, "test-model");
        runner.setProperty(InvokeLLMProcessor.SYSTEM_PROMPT, "system prompt");
        runner.setProperty(InvokeLLMProcessor.USER_PROMPT_TEMPLATE, "${llm.input}");
        runner.setProperty(InvokeLLMProcessor.MAX_TOKENS, "128");
        runner.setProperty(InvokeLLMProcessor.OUTPUT_MODE, "REPLACE_CONTENT");
        runner.setProperty(InvokeLLMProcessor.OUTPUT_ATTRIBUTE_NAME, "llm.response");
    }

    @Test
    public void testReplaceContentModeWithMockedClaudeProvider() throws Exception {
        LLMProvider mocked = mock(LLMProvider.class);
        when(mocked.complete("system prompt", "input payload")).thenReturn("llm response");
        processor.setProvider(mocked);

        runner.enqueue("input payload".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeLLMProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(InvokeLLMProcessor.REL_SUCCESS);
        success.get(0).assertContentEquals("llm response");
    }

    @Test
    public void testSetAttributeMode() throws Exception {
        LLMProvider mocked = mock(LLMProvider.class);
        when(mocked.complete("system prompt", "input payload")).thenReturn("attribute response");
        processor.setProvider(mocked);

        runner.setProperty(InvokeLLMProcessor.OUTPUT_MODE, "SET_ATTRIBUTE");
        runner.setProperty(InvokeLLMProcessor.OUTPUT_ATTRIBUTE_NAME, "my.llm.attr");
        runner.enqueue("input payload".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeLLMProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> success = runner.getFlowFilesForRelationship(InvokeLLMProcessor.REL_SUCCESS);
        MockFlowFile flowFile = success.get(0);
        assertEquals("my.llm.attr", "attribute response", flowFile.getAttribute("my.llm.attr"));
    }

    @Test
    public void testRateLimitRoutesToThrottled() throws Exception {
        LLMProvider mocked = mock(LLMProvider.class);
        when(mocked.complete("system prompt", "input payload")).thenThrow(new RateLimitException("rate_limited"));
        processor.setProvider(mocked);

        runner.enqueue("input payload".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeLLMProcessor.REL_THROTTLED, 1);
        List<MockFlowFile> throttled = runner.getFlowFilesForRelationship(InvokeLLMProcessor.REL_THROTTLED);
        assertEquals("rate_limited", throttled.get(0).getAttribute("llm.error"));
    }

    @Test
    public void testMissingApiKeyRoutesToFailure() {
        runner.setProperty(InvokeLLMProcessor.LLM_PROVIDER, "OPENAI");
        runner.removeProperty(InvokeLLMProcessor.API_KEY);
        runner.enqueue("input payload".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeLLMProcessor.REL_FAILURE, 1);
        List<MockFlowFile> failure = runner.getFlowFilesForRelationship(InvokeLLMProcessor.REL_FAILURE);
        String error = failure.get(0).getAttribute("llm.error");
        assertTrue(error != null && error.contains("API Key is required"));
    }

    private static class TestableInvokeLLMProcessor extends InvokeLLMProcessor {
        private LLMProvider provider;

        void setProvider(LLMProvider provider) {
            this.provider = provider;
        }

        @Override
        protected LLMProvider createProvider(String providerName, String apiKey, String apiBaseUrl, String model, int maxTokens) {
            if (provider != null) {
                return provider;
            }
            return super.createProvider(providerName, apiKey, apiBaseUrl, model, maxTokens);
        }
    }
}
