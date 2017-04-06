package com.lordofthejars.nosqlunit.vault;

import com.bettercloud.vault.VaultConfig;

import java.io.InputStream;

public class DefaultVaultInsertionStrategy implements com.lordofthejars.nosqlunit.core.InsertionStrategy<VaultClientCallback> {

    @Override
    public void insert(VaultClientCallback connection, InputStream dataset) throws Throwable {

        final VaultConfig vaultConfig = connection.vaultConfiguration();
        VaultConnection vaultConnection = new VaultConnection(vaultConfig);
        DataLoader dataLoader = new DataLoader(vaultConnection);
        dataLoader.load(dataset);

    }
}
