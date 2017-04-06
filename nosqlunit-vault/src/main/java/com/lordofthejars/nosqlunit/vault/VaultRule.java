package com.lordofthejars.nosqlunit.vault;

import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.lordofthejars.nosqlunit.core.AbstractNoSqlTestRule;
import com.lordofthejars.nosqlunit.core.DatabaseOperation;

public class VaultRule extends AbstractNoSqlTestRule {

    private static final String EXTENSION = "yml";

    private DatabaseOperation<VaultConfig> databaseOperation;

    public VaultRule(final VaultConfiguration configuration) {
        super(configuration.getConnectionIdentifier());
        databaseOperation = new VaultOperation(toVaultConfig(configuration));
    }

    public VaultRule(final VaultConfiguration configuration, final Object target) {
        super(configuration.getConnectionIdentifier());
        setTarget(target);
        databaseOperation = new VaultOperation(toVaultConfig(configuration));
    }

    @Override
    public DatabaseOperation getDatabaseOperation() {
        return databaseOperation;
    }

    @Override
    public String getWorkingExtension() {
        return EXTENSION;
    }

    @Override
    public void close() {
    }

    private VaultConfig toVaultConfig(VaultConfiguration vaultConfiguration) {
        VaultConfig vaultConfig = new VaultConfig();

        try {
            if (vaultConfiguration.getSslPemUTF8Contents() != null) {
                vaultConfig
                        .sslPemUTF8(vaultConfiguration.getSslPemUTF8Contents());
            }

            if (vaultConfiguration.getSslPemFile() != null) {
                    vaultConfig.sslPemFile(vaultConfiguration.getSslPemFile());
            }

            if (vaultConfiguration.getSslPemResource() != null) {
                vaultConfig.sslPemResource(vaultConfiguration.getSslPemResource());
            }

            if (vaultConfiguration.getReadTimeout() != null) {
                vaultConfig.readTimeout(vaultConfiguration.getReadTimeout());
            }

            if (vaultConfiguration.getOpenTimeout() != null) {
                vaultConfig.openTimeout(vaultConfiguration.getOpenTimeout());
            }

            if (vaultConfiguration.getAddress() != null) {
                vaultConfig.address(vaultConfiguration.getAddress());
            }

            if (vaultConfiguration.getToken() != null) {
                vaultConfig.token(vaultConfiguration.getToken());
            }

            if (vaultConfiguration.isSslVerify() != null) {
                vaultConfig.sslVerify(vaultConfiguration.isSslVerify());
            }

            return vaultConfig;
        } catch (VaultException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static VaultRule defaultRemoteVault(String host, String token) {
        VaultConfiguration vaultConfiguration = new VaultConfiguration(host);
        vaultConfiguration.setToken(token);
        return new VaultRule(vaultConfiguration);
    }

}
