package com.lordofthejars.nosqlunit.redis.embedded;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBlock;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.TransactionBlock;

public class TransactionServerOperations {

    private EmbeddedJedis jedis;

    public TransactionServerOperations(EmbeddedJedis jedis) {
        this.jedis = jedis;
    }

    public String watch(final byte[]... keys) {
        return "OK";
    }

    public String unwatch() {
        return "OK";
    }

    /**
     * Starts a pipeline, which is a very efficient way to send lots of command
     * and read all the responses when you finish sending them. Try to avoid
     * this version and use pipelined() when possible as it will give better
     * performance.
     * 
     * @param jedisPipeline
     * @return The results of the command in the same order you've run them.
     */
    public List<Object> pipelined(final PipelineBlock jedisPipeline) {
        return new ArrayList<Object>();
    }

    public Pipeline pipelined() {
        throw new IllegalStateException(
                "This version of NoSQLUnit updates to last version (2.2.1) of Jedis, which implies a modification of the whole pipeline interface, in future versions thisinterface will be adapted. If you want to use the old embed pipeline you can instanciate it manually new EmbeddedPipeline(jedis) where jedis is an embed jedis instance.");
        // return new EmbeddedPipeline(jedis);
    }

    public Transaction multi() {
        return null;
    }

    public List<Object> multi(final TransactionBlock jedisTransaction) {
        return new ArrayList<Object>();
    }

}
