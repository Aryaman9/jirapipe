package com.jirapipe.unit;

import com.jirapipe.config.JiraPipeProperties;
import com.jirapipe.webhook.WebhookSignatureValidator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WebhookSignatureValidatorTest {

    @Test
    void validatesCorrectly_whenNoSecretConfigured() {
        JiraPipeProperties properties = new JiraPipeProperties();
        properties.getJira().setWebhookSecret("");

        WebhookSignatureValidator validator = new WebhookSignatureValidator(properties);
        assertTrue(validator.isValid("any payload", null));
    }

    @Test
    void rejectsNullSignature_whenSecretConfigured() {
        JiraPipeProperties properties = new JiraPipeProperties();
        properties.getJira().setWebhookSecret("my-secret");

        WebhookSignatureValidator validator = new WebhookSignatureValidator(properties);
        assertFalse(validator.isValid("payload", null));
    }

    @Test
    void rejectsEmptySignature_whenSecretConfigured() {
        JiraPipeProperties properties = new JiraPipeProperties();
        properties.getJira().setWebhookSecret("my-secret");

        WebhookSignatureValidator validator = new WebhookSignatureValidator(properties);
        assertFalse(validator.isValid("payload", ""));
    }

    @Test
    void rejectsInvalidSignature() {
        JiraPipeProperties properties = new JiraPipeProperties();
        properties.getJira().setWebhookSecret("my-secret");

        WebhookSignatureValidator validator = new WebhookSignatureValidator(properties);
        assertFalse(validator.isValid("payload", "sha256=invalidhash"));
    }
}
