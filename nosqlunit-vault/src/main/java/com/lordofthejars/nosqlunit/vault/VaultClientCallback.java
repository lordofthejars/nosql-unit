package com.lordofthejars.nosqlunit.vault;

import com.bettercloud.vault.Vault;

public interface VaultClientCallback {

    Vault createClient();

}
