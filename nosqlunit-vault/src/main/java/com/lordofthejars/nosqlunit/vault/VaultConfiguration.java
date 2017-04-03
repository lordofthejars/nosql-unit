package com.lordofthejars.nosqlunit.vault;

import java.io.File;
import java.net.URL;

public class VaultConfiguration {

    private URL address;
    private String token;
    private int openTimeout;
    private int readTimeout;

    private File sslPemFile;
    private String sslPemResource;
    private String sslPemUTF8Contents;

    private boolean sslVerify;

    public VaultConfiguration(URL address) {
        this.address = address;
    }

    public URL getAddress() {
        return address;
    }

    public void setAddress(URL address) {
        this.address = address;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public int getOpenTimeout() {
        return openTimeout;
    }

    public void setOpenTimeout(int openTimeout) {
        this.openTimeout = openTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public File getSslPemFile() {
        return sslPemFile;
    }

    public void setSslPemFile(File sslPemFile) {
        this.sslPemFile = sslPemFile;
    }

    public String getSslPemResource() {
        return sslPemResource;
    }

    public void setSslPemResource(String sslPemResource) {
        this.sslPemResource = sslPemResource;
    }

    public String getSslPemUTF8Contents() {
        return sslPemUTF8Contents;
    }

    public void setSslPemUTF8Contents(String sslPemUTF8Contents) {
        this.sslPemUTF8Contents = sslPemUTF8Contents;
    }

    public boolean isSslVerify() {
        return sslVerify;
    }

    public void setSslVerify(boolean sslVerify) {
        this.sslVerify = sslVerify;
    }
}
