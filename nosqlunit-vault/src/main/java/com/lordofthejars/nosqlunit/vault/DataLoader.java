package com.lordofthejars.nosqlunit.vault;

import com.bettercloud.vault.response.AuthResponse;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DataLoader {

    private static final String TOKENS = "tokens";

    private VaultConnection vault;

    public DataLoader(VaultConnection vault) {
        this.vault = vault;
    }

    public void load(InputStream inputStream) {
        final Yaml yaml = new Yaml();
        final Object load = yaml.load(inputStream);

        if (areOnlySecrets(load)) {
            final List<Map<String, Map<String, Object>>> secrets = (List) load;

            insertTokens(secrets);
            insertSecrets(secrets);
        }
    }

    private void insertSecrets(List<Map<String, Map<String, Object>>> secrets) {
        for (Map<String, Map<String, Object>> secret : secrets) {

            final Set<Map.Entry<String, Map<String, Object>>> entries = secret.entrySet();

            for (Map.Entry<String, Map<String, Object>> entry : entries) {
                String backend = entry.getKey();
                if (!TOKENS.equals(backend)) {
                    vault.writeLogical(backend, entry.getValue());
                }
            }
        }
    }

    private void insertTokens(List<Map<String, Map<String, Object>>> data) {
        final Optional<Map.Entry<String, Map<String, Object>>> tokens = data.stream()
                .flatMap(e -> e.entrySet().stream())
                .filter(e -> TOKENS.equals(e.getKey()))
                .findFirst();

        if (tokens.isPresent()) {
            final List<Map<String, Object>> tokenDefinitions = (List<Map<String, Object>>) tokens.get().getValue();

            for (final Map<String, Object> tokenDefinition : tokenDefinitions) {
                final AuthResponse authResponse = vault.createToken()
                        .displayName(asString(tokenDefinition, "displayName"))
                        .meta(asMapOfStrings(tokenDefinition, "meta"))
                        .noDefaultPolicy(asBoolean(tokenDefinition, "noDefaultPolicy"))
                        .noParent(asBoolean(tokenDefinition, "noParent"))
                        .policies(asListOfStrings(tokenDefinition, "policies"))
                        .uuid(asString(tokenDefinition, "uuid"))
                        .ttl(asInteger(tokenDefinition, "ttl"))
                        .numUses(asLong(tokenDefinition, "numUses"))
                        .create();

                if (tokenDefinition.containsKey("secrets")) {
                    List<Map<String, Map<String, Object>>> secrets = (List<Map<String, Map<String, Object>>>) tokenDefinition.get("secrets");
                    try {
                        vault.updateToken(authResponse.getAuthClientToken());
                        insertSecrets(secrets);
                    } finally {
                        vault.reconnectToOriginal();
                    }
                }

            }

        }
    }

    private boolean areOnlySecrets(Object load) {
        return load instanceof List;
    }

    private Integer asInteger(Map<String, Object> elements, String key) {
        if (elements.containsKey(key)) {
            return (Integer) elements.get(key);
        }

        return null;
    }

    private Long asLong(Map<String, Object> elements, String key) {
        if (elements.containsKey(key)) {
            if (elements.get(key) instanceof Integer) {
                ((Integer) elements.get(key)).longValue();
            } else {
                return (Long) elements.get(key);
            }
        }

        return null;
    }

    private Map<String, String> asMapOfStrings(Map<String, Object> elements, String key) {
        if (elements.containsKey(key)) {
            Map<String, Object> map = (Map<String, Object>) elements.get(key);

            return toStringMap(map);
        }

        return null;
    }

    private List<String> asListOfStrings(Map<String, Object> elements, String key) {
        if (elements.containsKey(key)) {
            return (List<String>) elements.get(key);
        }

        return null;
    }

    private Boolean asBoolean(Map<String, Object> elements, String key) {
        if (elements.containsKey(key)) {
            return (Boolean) elements.get(key);
        }

        return null;
    }

    private String asString(Map<String, Object> elements, String key) {
        if (elements.containsKey(key)) {
            return (String) elements.get(key);
        }

        return null;
    }

    private Map<String, String> toStringMap(Map<String, Object> elements) {
        return elements.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()));
    }

}
