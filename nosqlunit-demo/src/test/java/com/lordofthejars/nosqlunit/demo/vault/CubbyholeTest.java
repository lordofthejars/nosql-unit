package com.lordofthejars.nosqlunit.demo.vault;

import com.bettercloud.vault.VaultConfig;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.vault.VaultRule;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertThat;

public class CubbyholeTest {

    private static final String ROOT_TOKEN = "fff3c8b5-2722-d6ac-128c-b48a1258c06f";

    @Rule
    public VaultRule vaultRule = VaultRule.defaultRemoteVault("http://192.168.99.100:8200", ROOT_TOKEN);

    //This token is provided usually by cloud script in form of env variable
    private String tempToken = "c56a4180-65aa-42ec-a945-5fd21dec0538";

    @Test
    @UsingDataSet(locations = "cubbyhole-setup.yml", loadStrategy = LoadStrategyEnum.INSERT)
    public void should_get_username_password_in_secured_way() {
        VaultConfig vaultConfig = new VaultConfig();
        vaultConfig.address("http://192.168.99.100:8200");

        Cubbyhole cubbyhole = new Cubbyhole(vaultConfig, "temp");
        final Map<String, String> usernameAndPassword = cubbyhole.getUsernameAndPassword(tempToken);

        assertThat(usernameAndPassword.get("username"), CoreMatchers.is("ada"));
        assertThat(usernameAndPassword.get("password"), CoreMatchers.is("alexandra"));
    }

}
