package com.lordofthejars.nosqlunit.vault;

import com.lordofthejars.nosqlunit.core.FailureHandler;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.lordofthejars.nosqlunit.util.DeepEquals.deepEquals;

public class VaultAssertion {

    private static final String TOKENS = "tokens";

    public static void strictAssertEquals(final InputStream dataset, final VaultConnection vault) {

        final Yaml yaml = new Yaml();
        final Object load = yaml.load(dataset);

        if (areOnlySecrets(load)) {
            final List<Map<String, Map<String, Object>>> secrets = (List) load;

            checkSecretsRegisteredByTokens(secrets, vault);
            assertSecrets(secrets, vault);
        }

    }

    private static void checkSecretsRegisteredByTokens(List<Map<String, Map<String, Object>>> data, VaultConnection vault) {
        final Optional<Map.Entry<String, Map<String, Object>>> tokens = data.stream()
                .flatMap(e -> e.entrySet().stream())
                .filter(e -> TOKENS.equals(e.getKey()))
                .findFirst();

        if (tokens.isPresent()) {
            final List<Map<String, Object>> tokenDefinitions = (List<Map<String, Object>>) tokens.get().getValue();

            for (final Map<String, Object> tokenDefinition : tokenDefinitions) {

                if (tokenDefinition.containsKey("uuid")) {
                    final String uuid = asString(tokenDefinition, "uuid");
                    if (tokenDefinition.containsKey("secrets")) {
                        List<Map<String, Map<String, Object>>> secrets = (List<Map<String, Map<String, Object>>>) tokenDefinition.get("secrets");
                        try {
                            vault.updateToken(uuid);
                            assertSecrets(secrets, vault);
                        } finally {
                            vault.reconnectToOriginal();
                        }
                    }
                }
            }
        }
    }

    private static void assertSecrets(List<Map<String, Map<String, Object>>> expectedSecrets, VaultConnection vaultConnection) {
        for (Map<String, Map<String, Object>> secret : expectedSecrets) {

            final Set<Map.Entry<String, Map<String, Object>>> entries = secret.entrySet();

            for (Map.Entry<String, Map<String, Object>> entry : entries) {
                String backend = entry.getKey();
                if (! TOKENS.equals(backend)) {
                    final Map<String, String> real = vaultConnection.readLogical(backend);

                    final Map<String, String> expected = toStringMap(entry.getValue());
                    if (!deepEquals(real, expected)) {
                        throw FailureHandler.createFailure(
                                "Expected element # %s # is not found but # %s # was found.",
                                expected, real);
                    }
                }
            }
        }
    }

    private static boolean areOnlySecrets(Object load) {
        return load instanceof List;
    }
    private static String asString(Map<String, Object> elements, String key) {
        if (elements.containsKey(key)) {
            return (String) elements.get(key);
        }

        return null;
    }

    private static Map<String, String> toStringMap(Map<String, Object> elements) {
        return elements.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()));
    }

}
