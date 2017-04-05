package com.lordofthejars.nosqlunit.demo.vault;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;

import java.util.Map;

public class Cubbyhole {

    private VaultConfig vaultConfig;
    private String cubbyholeName;

    public Cubbyhole(VaultConfig vaultConfig, String cubbyholeName) {
        this.vaultConfig = vaultConfig;
        this.cubbyholeName = cubbyholeName;
    }

    public Map<String, String> getUsernameAndPassword(String tempToken) {

        this.vaultConfig.token(tempToken);

        Vault vault = new Vault(this.vaultConfig);

        try {
            final Map<String, String> data = vault.logical().read("cubbyhole/" + cubbyholeName).getData();
            final String permToken = data.get("permtoken");

            this.vaultConfig.token(permToken);
            vault = new Vault(this.vaultConfig);
            return vault.logical().read("secret/bar").getData();

        } catch (VaultException e) {
            throw new RuntimeException(e);
        }
    }

}
