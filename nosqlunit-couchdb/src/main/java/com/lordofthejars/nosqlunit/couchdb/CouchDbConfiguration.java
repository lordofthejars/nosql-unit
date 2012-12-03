package com.lordofthejars.nosqlunit.couchdb;


import org.apache.http.conn.ssl.SSLSocketFactory;
import org.ektorp.CouchDbConnector;

import com.lordofthejars.nosqlunit.core.AbstractJsr330Configuration;

public class CouchDbConfiguration extends AbstractJsr330Configuration {

	private String url = "http://localhost:5984";
	
	private String username;
	private String password;
	
	private boolean caching = true;
	
	private boolean enableSsl = false;
	private boolean relaxedSsl = false;
	private SSLSocketFactory sslSocketFactory;
	
	private String databaseName;

	private CouchDbConnector couchDbConnector;
	
	public boolean isUsernameSet() {
		return username != null;
	}
	
	public boolean isPasswordSet() {
		return password != null;
	}
	
	public boolean isSslServerSocketSet() {
		return sslSocketFactory != null;
	}
	
	public boolean isDatabaseNameSet() {
		return databaseName != null;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isCaching() {
		return caching;
	}

	public void setCaching(boolean caching) {
		this.caching = caching;
	}

	public boolean isEnableSsl() {
		return enableSsl;
	}

	public void setEnableSsl(boolean enableSsl) {
		this.enableSsl = enableSsl;
	}

	public boolean isRelaxedSsl() {
		return relaxedSsl;
	}

	public void setRelaxedSsl(boolean relaxedSsl) {
		this.relaxedSsl = relaxedSsl;
	}

	public SSLSocketFactory getSslSocketFactory() {
		return sslSocketFactory;
	}

	public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
		this.sslSocketFactory = sslSocketFactory;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	public CouchDbConnector getCouchDbConnector() {
		return couchDbConnector;
	}
	
	public void setCouchDbConnector(CouchDbConnector couchDbConnector) {
		this.couchDbConnector = couchDbConnector;
	}
	
}
