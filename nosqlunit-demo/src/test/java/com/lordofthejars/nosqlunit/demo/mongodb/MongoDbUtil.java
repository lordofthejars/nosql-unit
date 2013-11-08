package com.lordofthejars.nosqlunit.demo.mongodb;

import com.mongodb.MongoClient;
import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;

public class MongoDbUtil {
    
    private static DB database;
    
    static{
        try {
            MongoClient mongo=new MongoClient("localhost",27017);
            database=mongo.getDB("test");
        } catch (UnknownHostException ex) {
            throw new IllegalArgumentException(ex);
        } catch (MongoException ex) {
        	throw new IllegalArgumentException(ex);
        }
    }
    
    public static DBCollection getCollection(String collectionName){
        return database.getCollection(collectionName);
    }
}
