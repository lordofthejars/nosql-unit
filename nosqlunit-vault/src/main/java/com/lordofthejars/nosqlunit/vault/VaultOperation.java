package com.lordofthejars.nosqlunit.vault;

import com.bettercloud.vault.Vault;
import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import java.io.InputStream;

public class VaultOperation extends AbstractCustomizableDatabaseOperation<VaultClientCallback, Vault> {

    private final Vault vault;

    public VaultOperation(final Vault client) {
        vault = client;
        setInsertionStrategy(new DefaultVaultInsertionStrategy());
        setComparisonStrategy(new DefaultVaultComparisionStrategy());
    }

    @Override public void insert(InputStream dataScript) {
        insertData(dataScript);
    }

    private void insertData(final InputStream dataScript) {
        try {
            executeInsertion(() -> vault, dataScript);
        } catch (final Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override public void deleteAll() {
        // TODO
    }

    @Override public boolean databaseIs(InputStream expectedData) {
        return compareData(expectedData);
    }

    private boolean compareData(final InputStream expectedData) throws NoSqlAssertionError {
        try {
            return executeComparison(() -> vault, expectedData);
        } catch (final NoSqlAssertionError e) {
            throw e;
        } catch (final Throwable e) {
            throw new IllegalStateException(e);
        }
    }

    @Override public Vault connectionManager() {
        return vault;
    }
}
