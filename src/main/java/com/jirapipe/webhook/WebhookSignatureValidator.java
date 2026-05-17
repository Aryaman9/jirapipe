package com.jirapipe.webhook;

import com.jirapipe.config.JiraPipeProperties;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

@Component
public class WebhookSignatureValidator {

    private static final String HMAC_SHA256 = "HmacSHA256";
    private final JiraPipeProperties properties;

    public WebhookSignatureValidator(JiraPipeProperties properties) {
        this.properties = properties;
    }

    public boolean isValid(String payload, String signature) {
        String secret = properties.getJira().getWebhookSecret();
        if (secret == null || secret.isBlank()) {
            return true;
        }
        if (signature == null || signature.isBlank()) {
            return false;
        }

        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec keySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_SHA256);
            mac.init(keySpec);
            byte[] hash = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            String computed = HexFormat.of().formatHex(hash);
            return computed.equalsIgnoreCase(signature.replace("sha256=", ""));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            return false;
        }
    }
}
