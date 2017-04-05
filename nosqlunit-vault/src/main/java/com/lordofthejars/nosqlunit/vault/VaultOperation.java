package com.lordofthejars.nosqlunit.vault;

import com.bettercloud.vault.VaultConfig;
import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

import java.io.InputStream;

public class VaultOperation extends AbstractCustomizableDatabaseOperation<VaultClientCallback, VaultConfig> {

    private final VaultConfig vaultConfig;

    public VaultOperation(final VaultConfig client) {
        vaultConfig = client;
        setInsertionStrategy(new DefaultVaultInsertionStrategy());
        setComparisonStrategy(new DefaultVaultComparisionStrategy());
    }

    @Override
    public void insert(InputStream dataScript) {
        insertData(dataScript);
    }

    private void insertData(final InputStream dataScript) {
        try {
            executeInsertion(() -> vaultConfig, dataScript);
        } catch (final Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void deleteAll() {
        // Need some deep refactor since you cannot delete everything in Vault. You need to provide some elements.
    }

    @Override
    public boolean databaseIs(InputStream expectedData) {
        return compareData(expectedData);
    }

    private boolean compareData(final InputStream expectedData) throws NoSqlAssertionError {
        try {
            return executeComparison(() -> vaultConfig, expectedData);
        } catch (final NoSqlAssertionError e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public VaultConfig connectionManager() {
        return vaultConfig;
    }
}
