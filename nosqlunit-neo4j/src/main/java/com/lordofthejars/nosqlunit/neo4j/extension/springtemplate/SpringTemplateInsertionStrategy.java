package com.lordofthejars.nosqlunit.neo4j.extension.springtemplate;

import java.io.InputStream;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.data.neo4j.config.JtaTransactionManagerFactoryBean;
import org.springframework.data.neo4j.support.Neo4jTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.lordofthejars.nosqlunit.neo4j.Neo4jConnectionCallback;
import com.lordofthejars.nosqlunit.neo4j.Neo4jInsertionStrategy;

public class SpringTemplateInsertionStrategy implements Neo4jInsertionStrategy {

	@Override
	public void insert(Neo4jConnectionCallback connection, InputStream dataset) throws Throwable {
		
		Neo4jTemplate neo4jTemplate = neo4jTemplate(connection);
		List<Object> readValues = readObjects(dataset);
		insertObjectsInTransaction(neo4jTemplate, readValues);
		
	}

	private void insertObjectsInTransaction(final Neo4jTemplate neo4jTemplate, final List<Object> readValues) {
		TransactionTemplate transactionalTemplate = transactionalTemplate(neo4jTemplate.getGraphDatabaseService());
		transactionalTemplate.execute(new TransactionCallback<Void>() {

			@Override
			public Void doInTransaction(TransactionStatus status) {
				for (Object object : readValues) {
					neo4jTemplate.save(object);
				}
				
				return null;
			}
		});
		
	}

	private List<Object> readObjects(InputStream dataset) {
		DataParser dataParser = new DataParser();
		List<Object> readValues = dataParser.readValues(dataset);
		return readValues;
	}

	private TransactionTemplate transactionalTemplate(GraphDatabaseService graphDatabaseService) {
		
		try {
			JtaTransactionManagerFactoryBean jtaTransactionManagerFactoryBean = new JtaTransactionManagerFactoryBean(graphDatabaseService);
			return new TransactionTemplate(jtaTransactionManagerFactoryBean.getObject());
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
		
	}
	
	private Neo4jTemplate neo4jTemplate(Neo4jConnectionCallback connection) {
		GraphDatabaseService graphDatabaseService = connection.graphDatabaseService();
		Neo4jTemplate neo4jTemplate = new Neo4jTemplate(graphDatabaseService);
		
		return neo4jTemplate;
	}

}
