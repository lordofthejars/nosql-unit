package com.lordofthejars.nosqlunit.vault;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

import java.io.File;
import java.net.URL;

public class VaultConfiguration extends AbstractJsr330Configuration {

    private String address;
    private String token;
    private Integer openTimeout;
    private Integer readTimeout;

    private File sslPemFile;
    private String sslPemResource;
    private String sslPemUTF8Contents;

    private Boolean sslVerify;

    public VaultConfiguration(String address) {
        this.address = address;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Integer getOpenTimeout() {
        return openTimeout;
    }

    public void setOpenTimeout(Integer openTimeout) {
        this.openTimeout = openTimeout;
    }

    public Integer getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Integer readTimeout) {
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

    public Boolean isSslVerify() {
        return sslVerify;
    }

    public void setSslVerify(Boolean sslVerify) {
        this.sslVerify = sslVerify;
    }
}
