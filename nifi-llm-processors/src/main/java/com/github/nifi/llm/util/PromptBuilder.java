package com.github.nifi.llm.util;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;

public final class PromptBuilder {

    private PromptBuilder() {
    }

    public static String build(String template, FlowFile flowFile, ProcessContext context) {
        if (template == null) {
            return "";
        }

        PropertyValue propertyValue = context.newPropertyValue(template);
        String resolved = propertyValue.evaluateAttributeExpressions(flowFile).getValue();
        if (resolved == null) {
            resolved = "";
        }

        String input = flowFile.getAttribute("llm.input");
        if (input == null) {
            input = "";
        }

        return resolved.replace("${llm.input}", input);
    }
}
