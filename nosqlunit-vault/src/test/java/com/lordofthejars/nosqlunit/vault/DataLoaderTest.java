package com.lordofthejars.nosqlunit.vault;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.api.Auth;
import com.bettercloud.vault.response.AuthResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DataLoaderTest {

    @Mock
    VaultConnection vaultConnection;

    @Mock
    Vault vault;

    @Mock
    Auth auth;

    @Mock
    AuthResponse response;

    @Test
    public void should_read_simple_secrets_definition() throws FileNotFoundException, VaultException {

        // Given:
        final DataLoader dataLoader = new DataLoader(vaultConnection);

        // When:
        dataLoader.load(new FileInputStream("src/test/resources/only_secrets.yml"));

        // Then:
        final Map<String, Object> fooSecret = new HashMap<>();
        fooSecret.put("zip", "zap");
        fooSecret.put("a", "b");

        final Map<String, Object> barSecret = new HashMap<>();
        barSecret.put("zap", "zip");
        barSecret.put("b", "a");

        verify(vaultConnection).writeLogical("secret/foo", fooSecret);
        verify(vaultConnection).writeLogical("secret/bar", barSecret);

    }

    @Test
    public void should_insert_tokens() throws FileNotFoundException, VaultException {

        // Given:

        when(vaultConnection.createToken()).thenReturn(new VaultConnection.TokenCreator(vault));
        when(vault.auth()).thenReturn(auth);

        when(response.getAuthClientToken()).thenReturn("aa-bb-cc-dd");

        when(auth.createToken(anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject()))
                .thenReturn(response);

        final DataLoader dataLoader = new DataLoader(vaultConnection);

        // When:
        dataLoader.load(new FileInputStream("src/test/resources/tokens_and_secrets.yml"));

        // Then:

        verify(vaultConnection, times(2)).createToken();
        verify(vaultConnection, times(2)).updateToken("aa-bb-cc-dd");
        verify(vaultConnection, times(2)).reconnectToOriginal();

        final Map<String, Object> fooSecret = new HashMap<>();
        fooSecret.put("zip", "zap");
        fooSecret.put("a", "b");

        verify(vaultConnection).writeLogical("secret/foo", fooSecret);
        verify(vaultConnection).writeLogical("secret/bar", fooSecret);

    }


}
