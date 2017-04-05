package com.lordofthejars.nosqlunit.vault;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.api.Auth;
import com.bettercloud.vault.response.AuthResponse;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
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
public class VaultAssertionTest {

    @Mock
    VaultConnection vaultConnection;

    @Mock
    Vault vault;

    @Mock
    Auth auth;

    @Mock
    AuthResponse response;

    @Test
    public void should_assert_same_secrets() throws FileNotFoundException {

        // Given:
        final Map<String, String> fooSecret = new HashMap<>();
        fooSecret.put("zip", "zap");
        fooSecret.put("a", "b");

        final Map<String, String> barSecret = new HashMap<>();
        barSecret.put("zap", "zip");
        barSecret.put("b", "a");

        when(vaultConnection.readLogical("secret/foo")).thenReturn(fooSecret);
        when(vaultConnection.readLogical("secret/bar")).thenReturn(barSecret);

        // When:
        VaultAssertion.strictAssertEquals(new FileInputStream("src/test/resources/only_secrets.yml"), vaultConnection);

        // Then:

        verify(vaultConnection).readLogical("secret/foo");
        verify(vaultConnection).readLogical("secret/bar");

    }

    @Test
    public void should_assert_same_secrets_with_tokens() throws FileNotFoundException, VaultException {

        // Given:
        when(vaultConnection.createToken()).thenReturn(new VaultConnection.TokenCreator(vault));
        when(vault.auth()).thenReturn(auth);

        when(auth.createToken(anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject()))
                .thenReturn(response);

        final Map<String, String> fooSecret = new HashMap<>();
        fooSecret.put("zip", "zap");
        fooSecret.put("a", "b");

        when(vaultConnection.readLogical("secret/foo")).thenReturn(fooSecret);

        // When:
        VaultAssertion.strictAssertEquals(new FileInputStream("src/test/resources/tokens_and_secrets.yml"), vaultConnection);

        // Then:
        verify(vaultConnection, times(1)).updateToken("C56A4180-65AA-42EC-A945-5FD21DEC0538");
        verify(vaultConnection, times(1)).reconnectToOriginal();

        verify(vaultConnection).readLogical("secret/foo");
        verify(vaultConnection, times(0)).readLogical("secret/bar");

    }

    @Test(expected = NoSqlAssertionError.class)
    public void should_fail_if_secrets_not_the_same() throws FileNotFoundException, VaultException {

        // Given:
        final Map<String, String> fooSecret = new HashMap<>();
        fooSecret.put("zip", "zap");
        fooSecret.put("a", "b");

        final Map<String, String> barSecret = new HashMap<>();
        barSecret.put("zip", "zip");
        barSecret.put("b", "a");

        when(vaultConnection.readLogical("secret/foo")).thenReturn(fooSecret);
        when(vaultConnection.readLogical("secret/bar")).thenReturn(barSecret);

        // When:
        VaultAssertion.strictAssertEquals(new FileInputStream("src/test/resources/only_secrets.yml"), vaultConnection);


    }

}
