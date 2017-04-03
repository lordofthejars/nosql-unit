package com.lordofthejars.nosqlunit.vault;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;

public class Sandbox {

    @Test
    public void run_root_token() throws VaultException {
        VaultConfig vaultConfig = new VaultConfig("http://192.168.99.100:8200", "01ef44d9-5e6e-4afc-baa5-884f9eec40e4");
        Vault vault = new Vault(vaultConfig);

        final Map<String, String> secrets = new HashMap<>();
        secrets.put("value", "world");

        vault.logical().write("secret/hello", secrets);

    }

    @Test
    public void run_cubbyhole() throws VaultException {
        // Cloud Init Script
        VaultConfig vaultConfig = new VaultConfig("http://192.168.99.100:8200", "01ef44d9-5e6e-4afc-baa5-884f9eec40e4");
        Vault vault = new Vault(vaultConfig);

        final AuthResponse token =
            vault.auth().createToken(UUID.randomUUID(), null, null, null, null, null, null, Integer.toUnsignedLong(2));

        System.out.println(token.getAuthClientToken());

        vaultConfig = new VaultConfig("http://192.168.99.100:8200", token.getAuthClientToken());
        vault = new Vault(vaultConfig);

        Map<String, String> tokens = new HashMap<>();
        tokens.put("token", "my_other_token");

        vault.logical().write("cubbyhole/service11", tokens);

        System.out.println(vault.logical().read("cubbyhole/service11").getData());
        System.out.println("XXXXXXX");
        System.out.println(vault.logical().read("cubbyhole/service11").getData());

    }


}
