package com.lordofthejars.nosqlunit.couchdb;

import java.net.MalformedURLException;

import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

public class CouchDbConnectorFactory {

	public static CouchDbConnector couchDbConnector(CouchDbConfiguration configuration) {

		if(!configuration.isDatabaseNameSet()) {
			throw new IllegalArgumentException("Database name should be provided.");
		}
		
		StdHttpClient.Builder httpBuilder = couchDbHttpClient(configuration);
		return couchDbConnector(configuration, httpBuilder);
		
	}

	private static CouchDbConnector couchDbConnector(CouchDbConfiguration configuration,
			StdHttpClient.Builder httpBuilder) {
		CouchDbInstance dbInstance = new StdCouchDbInstance(httpBuilder.build());
		return dbInstance.createConnector(configuration.getDatabaseName(), true);
	}

	private static StdHttpClient.Builder couchDbHttpClient(CouchDbConfiguration configuration) {
		StdHttpClient.Builder httpBuilder = new StdHttpClient.Builder();

		try {
			httpBuilder.url(configuration.getUrl()).caching(configuration.isCaching())
					.enableSSL(configuration.isEnableSsl()).relaxedSSLSettings(configuration.isRelaxedSsl());

		} catch (MalformedURLException e) {
			throw new IllegalArgumentException(e);
		}

		if(configuration.isUsernameSet()) {
			httpBuilder.username(configuration.getUsername());
		}
		
		if(configuration.isPasswordSet()) {
			httpBuilder.password(configuration.getPassword());
		}
		
		if(configuration.isSslServerSocketSet()) {
			httpBuilder.sslSocketFactory(configuration.getSslSocketFactory());
		}
		return httpBuilder;
	}

}
