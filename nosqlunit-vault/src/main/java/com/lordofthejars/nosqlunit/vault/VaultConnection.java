package com.lordofthejars.nosqlunit.vault;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class VaultConnection {

    private Vault vault;
    private VaultConfig originalConfig;

    public VaultConnection(VaultConfig vaultConfig) {
        this.originalConfig = vaultConfig;
        this.vault = new Vault(vaultConfig);
    }

    public void reconnectToOriginal() {
        this.vault = new Vault(originalConfig);
    }

    public void updateToken(String token) {
        final VaultConfig vaultConfig = recreateVaultConfig();
        vaultConfig.token(token);
        this.vault = new Vault(vaultConfig);
    }

    public void writeLogical(String path, Map<String, Object> nameValuePairs) {
        final Map<String, String> secretElementsAsString = nameValuePairs.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()));

        try {
            vault.logical().write(path, secretElementsAsString);
        } catch (VaultException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public Map<String, String> readLogical(String path) {
        try {
            return vault.logical().read(path).getData();
        } catch (VaultException e) {
            throw new IllegalStateException(e);
        }
    }

    public TokenCreator createToken() {
        return new TokenCreator(vault);
    }

    private VaultConfig recreateVaultConfig() {
        VaultConfig vaultConfig = new VaultConfig();
        vaultConfig.token(originalConfig.getToken());
        vaultConfig.address(originalConfig.getAddress());
        vaultConfig.openTimeout(originalConfig.getOpenTimeout());
        vaultConfig.readTimeout(originalConfig.getReadTimeout());
        vaultConfig.sslPemUTF8(originalConfig.getSslPemUTF8());

        return vaultConfig;
    }

    public static class TokenCreator {

        private Vault vault;

        private UUID uuid;
        private List<String> policies;
        private Map<String, String> meta;
        private Boolean noParent;
        private Boolean noDefaultPolicy;
        private String ttl;
        private String displayName;
        private Long numUses;

        TokenCreator(Vault vault) {
            this.vault = vault;
        }

        public TokenCreator uuid(String uuid) {
            if (uuid != null) {
                this.uuid = UUID.fromString(uuid);
            }
            return this;
        }

        public UUID getUuid() {
            return uuid;
        }

        public TokenCreator policies(List<String> policies) {
            this.policies = policies;
            return this;
        }

        public List<String> getPolicies() {
            return policies;
        }

        public TokenCreator meta(Map<String, String> meta) {
            this.meta = meta;
            return this;
        }

        public Map<String, String> getMeta() {
            return meta;
        }

        public TokenCreator noParent(Boolean noParent) {
            this.noParent = noParent;
            return this;
        }

        public Boolean getNoParent() {
            return noParent;
        }

        public TokenCreator noDefaultPolicy(Boolean noDefaultPolicy) {
            this.noDefaultPolicy = noDefaultPolicy;
            return this;
        }

        public Boolean getNoDefaultPolicy() {
            return noDefaultPolicy;
        }

        public TokenCreator ttl(Integer ttl) {
            if (ttl != null) {
                this.ttl = Integer.toString(ttl);
            }
            return this;
        }

        public String getTtl() {
            return ttl;
        }

        public TokenCreator displayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public String getDisplayName() {
            return displayName;
        }

        public TokenCreator numUses(Long numUses) {
            this.numUses = numUses;
            return this;
        }

        public Long getNumUses() {
            return numUses;
        }

        public AuthResponse create() {
            try {
                return vault.auth().createToken(uuid, policies, meta, noParent, noDefaultPolicy, ttl, displayName, numUses);
            } catch (VaultException e) {
                throw new IllegalStateException(e);
            }
        }
    }

}
