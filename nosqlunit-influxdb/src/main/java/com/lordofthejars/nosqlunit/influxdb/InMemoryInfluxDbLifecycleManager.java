
package com.lordofthejars.nosqlunit.influxdb;

import java.io.File;
import java.io.IOException;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

import io.apisense.embed.influx.InfluxServer;
import io.apisense.embed.influx.ServerAlreadyRunningException;
import io.apisense.embed.influx.configuration.InfluxConfigurationWriter;

public class InMemoryInfluxDbLifecycleManager extends AbstractLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryInfluxDb.class);

    private static final String LOCALHOST = "localhost";

    private static final int PORT = 8086;

    private static final int BACKUP_PORT = 8088;

    private static final String URL = String.format("http://%s:%d", LOCALHOST, PORT);

    public static final String INMEMORY_INFLUX_TARGET_PATH = "target" + File.separatorChar + "influx-data"
            + File.separatorChar + "impermanent-db";

    private String targetPath = INMEMORY_INFLUX_TARGET_PATH;

    private InfluxServer server;

    @Override
    public String getHost() {
        return LOCALHOST;
    }

    @Override
    public int getPort() {
        return PORT;
    }

    @Override
    public void doStart() throws Throwable {
        LOGGER.info("Starting EmbeddedInMemory InfluxDb instance.");
        EmbeddedInfluxInstancesFactory.getInstance().addEmbeddedInstance(embeddedInfluxDb(targetPath), targetPath);
        LOGGER.info("Started EmbeddedInMemory InfluxDb instance.");
    }

    private InfluxDB embeddedInfluxDb(final String targetPath)
            throws IOException, ServerAlreadyRunningException, InterruptedException {
        final InfluxConfigurationWriter config = new InfluxConfigurationWriter.Builder() //
                .setHttp(PORT) // be default auth is disabled
                .setBackupAndRestorePort(BACKUP_PORT) //
                .setDataPath(new File(targetPath)) //
                .build();

        final InfluxServer.Builder builder = new InfluxServer.Builder();
        builder.setInfluxConfiguration(config);
        server = builder.build();

        server.start();

        // wait for server to start
        Thread.sleep(5000);

        return InfluxDBFactory.connect(URL);

    }

    @Override
    public void doStop() {
        LOGGER.info("Stopping EmbeddedInMemory InfluxDb instance.");

        EmbeddedInfluxInstancesFactory.getInstance().removeEmbeddedInstance(targetPath);

        try {
            server.stop();
            LOGGER.info("Stopped EmbeddedInMemory InfluxDb instance.");
        } catch (final Exception e) {
            LOGGER.error("Exception in stopping EmbeddedInMemory InfluxDb instance", e);
        }
    }

    public void setTargetPath(final String targetPath) {
        this.targetPath = targetPath;
    }

    public String getTargetPath() {
        return targetPath;
    }

}
