package com.github.nifi.llm.processor;

import com.github.nifi.llm.service.ClaudeProvider;
import com.github.nifi.llm.service.GeminiProvider;
import com.github.nifi.llm.service.LLMProvider;
import com.github.nifi.llm.service.OllamaProvider;
import com.github.nifi.llm.service.OpenAIProvider;
import com.github.nifi.llm.service.RateLimitException;
import com.github.nifi.llm.util.PromptBuilder;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"llm", "ai", "claude", "gemini", "openai", "ollama", "enrich", "nlp"})
@CapabilityDescription("Sends FlowFile content to an LLM using a configurable prompt template, then writes the response to content or attributes.")
@SupportsBatching
public class InvokeLLMProcessor extends AbstractProcessor {

    static final String PROVIDER_CLAUDE = "CLAUDE";
    static final String PROVIDER_GEMINI = "GEMINI";
    static final String PROVIDER_OPENAI = "OPENAI";
    static final String PROVIDER_OLLAMA = "OLLAMA";

    static final String MODE_REPLACE_CONTENT = "REPLACE_CONTENT";
    static final String MODE_SET_ATTRIBUTE = "SET_ATTRIBUTE";
    static final String MODE_APPEND_CONTENT = "APPEND_CONTENT";

    private static final org.apache.nifi.components.Validator NON_EMPTY_VALIDATOR = (subject, input, context) -> {
        boolean valid = input != null && !input.trim().isEmpty();
        return new org.apache.nifi.components.ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(valid)
                .explanation(valid ? null : "Value must not be empty")
                .build();
    };

    private static final org.apache.nifi.components.Validator OPTIONAL_NON_EMPTY_VALIDATOR = (subject, input, context) -> {
        return new org.apache.nifi.components.ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(true)
                .build();
    };

    private static final org.apache.nifi.components.Validator INTEGER_VALIDATOR = (subject, input, context) -> {
        boolean valid = true;
        if (input == null || input.trim().isEmpty()) {
            valid = false;
        } else {
            try {
                Integer.parseInt(input.trim());
            } catch (NumberFormatException e) {
                valid = false;
            }
        }
        return new org.apache.nifi.components.ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(valid)
                .explanation(valid ? null : "Value must be a valid integer")
                .build();
    };

    public static final PropertyDescriptor LLM_PROVIDER = new PropertyDescriptor.Builder()
            .name("LLM Provider")
            .description("Which LLM backend to use")
            .required(true)
            .allowableValues(PROVIDER_CLAUDE, PROVIDER_GEMINI, PROVIDER_OPENAI, PROVIDER_OLLAMA)
            .defaultValue(PROVIDER_CLAUDE)
            .build();

    public static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API Key")
            .description("API key for remote providers (not required for Ollama)")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(OPTIONAL_NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor API_BASE_URL = new PropertyDescriptor.Builder()
            .name("API Base URL")
            .description("Override base URL (used for Ollama or proxies)")
            .required(true)
            .defaultValue("http://localhost:11434")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MODEL = new PropertyDescriptor.Builder()
            .name("Model Name")
            .description("Provider-specific model identifier")
            .required(true)
            .defaultValue("claude-3-5-sonnet-20241022")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SYSTEM_PROMPT = new PropertyDescriptor.Builder()
            .name("System Prompt")
            .description("Static system instruction for the LLM")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER_PROMPT_TEMPLATE = new PropertyDescriptor.Builder()
            .name("User Prompt Template")
            .description("Use ${llm.input} to reference FlowFile content. Supports NiFi EL.")
            .required(true)
            .defaultValue("${llm.input}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_TOKENS = new PropertyDescriptor.Builder()
            .name("Max Tokens")
            .description("Maximum tokens in the LLM response")
            .required(true)
            .defaultValue("1024")
            .addValidator(INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_MODE = new PropertyDescriptor.Builder()
            .name("Output Mode")
            .description("How to write the LLM response back to the FlowFile")
            .required(true)
            .allowableValues(MODE_REPLACE_CONTENT, MODE_SET_ATTRIBUTE, MODE_APPEND_CONTENT)
            .defaultValue(MODE_REPLACE_CONTENT)
            .build();

    public static final PropertyDescriptor OUTPUT_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Output Attribute Name")
            .description("Attribute name when OUTPUT_MODE is SET_ATTRIBUTE")
            .required(true)
            .defaultValue("llm.response")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile successfully processed by LLM")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFile failed (HTTP error, parse error, auth failure)")
            .build();

    public static final Relationship REL_THROTTLED = new Relationship.Builder()
            .name("throttled")
            .description("LLM returned rate-limit response (HTTP 429)")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final org.apache.nifi.processor.ProcessorInitializationContext context) {
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
        propertyDescriptors.add(LLM_PROVIDER);
        propertyDescriptors.add(API_KEY);
        propertyDescriptors.add(API_BASE_URL);
        propertyDescriptors.add(MODEL);
        propertyDescriptors.add(SYSTEM_PROMPT);
        propertyDescriptors.add(USER_PROMPT_TEMPLATE);
        propertyDescriptors.add(MAX_TOKENS);
        propertyDescriptors.add(OUTPUT_MODE);
        propertyDescriptors.add(OUTPUT_ATTRIBUTE_NAME);
        this.descriptors = Collections.unmodifiableList(propertyDescriptors);

        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        relationshipSet.add(REL_THROTTLED);
        this.relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        String provider = context.getProperty(LLM_PROVIDER).getValue();
        String model = context.getProperty(MODEL).evaluateAttributeExpressions().getValue();
        String apiKey = context.getProperty(API_KEY).evaluateAttributeExpressions().getValue();

        if (!PROVIDER_OLLAMA.equalsIgnoreCase(provider) && isBlank(apiKey)) {
            getLogger().warn("API Key is not configured for provider {}. FlowFiles will be routed to failure.",
                    new Object[]{provider});
        }

        getLogger().info("InvokeLLMProcessor scheduled with provider={} model={}", new Object[]{provider, model});
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startMs = System.currentTimeMillis();

        try {
            final String originalContent = readFlowFileContent(session, flowFile);
            flowFile = session.putAttribute(flowFile, "llm.input", originalContent);

            final String providerName = context.getProperty(LLM_PROVIDER).getValue();
            final String apiKey = context.getProperty(API_KEY).evaluateAttributeExpressions().getValue();
            final String apiBaseUrl = context.getProperty(API_BASE_URL).evaluateAttributeExpressions().getValue();
            final String model = context.getProperty(MODEL).evaluateAttributeExpressions().getValue();
            final int maxTokens = context.getProperty(MAX_TOKENS).asInteger();
            final String systemPrompt = context.getProperty(SYSTEM_PROMPT).evaluateAttributeExpressions(flowFile).getValue();
            final String userPromptTemplate = context.getProperty(USER_PROMPT_TEMPLATE).evaluateAttributeExpressions(flowFile).getValue();
            final String resolvedUserPrompt = PromptBuilder.build(userPromptTemplate, flowFile, context);

            if (!PROVIDER_OLLAMA.equalsIgnoreCase(providerName) && isBlank(apiKey)) {
                flowFile = session.putAttribute(flowFile, "llm.error", "API Key is required for provider " + providerName);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final LLMProvider provider = createProvider(providerName, apiKey, apiBaseUrl, model, maxTokens);

            final String response;
            try {
                response = provider.complete(systemPrompt, resolvedUserPrompt);
            } catch (RateLimitException rateLimitException) {
                flowFile = session.putAttribute(flowFile, "llm.error", "rate_limited");
                flowFile = session.putAttribute(flowFile, "llm.provider", providerName);
                flowFile = session.putAttribute(flowFile, "llm.model", model);
                flowFile = session.putAttribute(flowFile, "llm.processingTimeMs", String.valueOf(System.currentTimeMillis() - startMs));
                session.transfer(flowFile, REL_THROTTLED);
                return;
            } catch (IOException ioException) {
                flowFile = session.putAttribute(flowFile, "llm.error", safeErrorMessage(ioException));
                flowFile = session.putAttribute(flowFile, "llm.provider", providerName);
                flowFile = session.putAttribute(flowFile, "llm.model", model);
                flowFile = session.putAttribute(flowFile, "llm.processingTimeMs", String.valueOf(System.currentTimeMillis() - startMs));
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final String outputMode = context.getProperty(OUTPUT_MODE).getValue();
            if (MODE_REPLACE_CONTENT.equalsIgnoreCase(outputMode)) {
                final byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
                flowFile = session.write(flowFile, out -> out.write(bytes));
            } else if (MODE_SET_ATTRIBUTE.equalsIgnoreCase(outputMode)) {
                final String outputAttributeName = context.getProperty(OUTPUT_ATTRIBUTE_NAME)
                        .evaluateAttributeExpressions(flowFile)
                        .getValue();
                flowFile = session.putAttribute(flowFile, outputAttributeName, response);
            } else if (MODE_APPEND_CONTENT.equalsIgnoreCase(outputMode)) {
                final String appended;
                if (originalContent == null || originalContent.isEmpty()) {
                    appended = response;
                } else {
                    appended = originalContent + System.lineSeparator() + response;
                }
                final byte[] bytes = appended.getBytes(StandardCharsets.UTF_8);
                flowFile = session.write(flowFile, out -> out.write(bytes));
            }

            final Map<String, String> outputAttributes = new HashMap<>();
            outputAttributes.put("llm.provider", providerName);
            outputAttributes.put("llm.model", model);
            outputAttributes.put("llm.processingTimeMs", String.valueOf(System.currentTimeMillis() - startMs));
            flowFile = session.putAllAttributes(flowFile, outputAttributes);

            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception exception) {
            getLogger().error("Failed to process FlowFile with LLM", exception);
            flowFile = session.putAttribute(flowFile, "llm.error", safeErrorMessage(exception));
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    protected LLMProvider createProvider(String providerName, String apiKey, String apiBaseUrl, String model, int maxTokens) {
        if (providerName == null) {
            throw new IllegalArgumentException("Provider cannot be null");
        }

        switch (providerName.toUpperCase()) {
            case PROVIDER_CLAUDE:
                return new ClaudeProvider(apiKey, model, maxTokens);
            case PROVIDER_GEMINI:
                return new GeminiProvider(apiKey, model);
            case PROVIDER_OPENAI:
                return new OpenAIProvider(apiKey, model, maxTokens);
            case PROVIDER_OLLAMA:
                return new OllamaProvider(apiBaseUrl, model);
            default:
                throw new IllegalArgumentException("Unsupported provider: " + providerName);
        }
    }

    private String readFlowFileContent(ProcessSession session, FlowFile flowFile) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        session.read(flowFile, inputStream -> copy(inputStream, outputStream));
        return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
    }

    private void copy(InputStream inputStream, ByteArrayOutputStream outputStream) throws IOException {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
        }
    }

    private String safeErrorMessage(Throwable throwable) {
        if (throwable == null) {
            return "Unknown error";
        }
        String message = throwable.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return throwable.getClass().getSimpleName();
        }
        return message;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
