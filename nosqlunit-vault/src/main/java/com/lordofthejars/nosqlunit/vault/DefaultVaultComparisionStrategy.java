package com.lordofthejars.nosqlunit.vault;

import com.lordofthejars.nosqlunit.core.ComparisonStrategy;
import java.io.InputStream;

public class DefaultVaultComparisionStrategy implements ComparisonStrategy<VaultClientCallback> {

    @Override
    public boolean compare(VaultClientCallback connection, InputStream dataset)
        throws  Throwable {
        return false;
    }

    @Override
    public void setIgnoreProperties(String[] ignoreProperties) {

    }
}
