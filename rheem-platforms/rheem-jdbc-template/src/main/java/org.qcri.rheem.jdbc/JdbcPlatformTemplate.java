package org.qcri.rheem.jdbc;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.jdbc.channels.SqlQueryChannel;
import org.qcri.rheem.jdbc.execution.DatabaseDescriptor;
import org.qcri.rheem.jdbc.execution.JdbcExecutor;
import org.qcri.rheem.jdbc.operators.SqlToStreamOperator;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * {@link Platform} implementation for the PostgreSQL database.
 */
public abstract class JdbcPlatformTemplate extends Platform implements Plugin {

    public final String cpuMhzProperty = String.format("rheem.%s.cpu.mhz", this.getPlatformId());

    public final String coresProperty = String.format("rheem.%s.cores", this.getPlatformId());

    public final String hdfsMsPerMbProperty = String.format("rheem.%s.hdfs.ms-per-mb", this.getPlatformId());

    public final String jdbcUrlProperty = String.format("rheem.%s.jdbc.url", this.getPlatformId());

    public final String jdbcUserProperty = String.format("rheem.%s.jdbc.user", this.getPlatformId());

    public final String jdbcPasswordProperty = String.format("rheem.%s.jdbc.password", this.getPlatformId());

    private String getDefaultConfigurationFile() {
        return String.format("rheem-%s-defaults.properties", this.getPlatformId());
    }

    /**
     * {@link ChannelDescriptor} for {@link SqlQueryChannel}s with this instance.
     */
    private final SqlQueryChannel.Descriptor sqlQueryChannelDescriptor = new SqlQueryChannel.Descriptor(this);

    protected final Collection<Mapping> mappings = new LinkedList<>();

    public Connection getConnection() {
        return connection;
    }

    private Connection connection = null;

    protected JdbcPlatformTemplate(String platformName) {
        super(platformName);
        this.initializeMappings();
    }

    @Override
    public void configureDefaults(Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(this.getDefaultConfigurationFile()));
    }

    protected abstract void initializeMappings();

    @Override
    public Collection<Mapping> getMappings() {
        return this.mappings;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return Collections.singleton(new DefaultChannelConversion(
                this.getSqlQueryChannelDescriptor(),
                StreamChannel.DESCRIPTOR,
                () -> new SqlToStreamOperator(this)
        ));
    }

    @Override
    public void setProperties(Configuration configuration) {
        // Nothing to do, because we already configured the properties in #configureDefaults(...).
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(this, JavaPlatform.getInstance());
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new JdbcExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration) {
        int cpuMhz = (int) configuration.getLongProperty(this.cpuMhzProperty);
        int numCores = (int) configuration.getLongProperty(this.coresProperty);
        double hdfsMsPerMb = configuration.getDoubleProperty(this.hdfsMsPerMbProperty);
        return LoadProfileToTimeConverter.createDefault(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000.)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
    }

    /**
     * Provide a unique identifier for this kind of platform. Should consist of alphanumerical characters only.
     *
     * @return the platform ID
     */
    public abstract String getPlatformId();

    /**
     * Provide the name of the JDBC driver {@link Class} for this instance.
     *
     * @return the driver {@link Class} name
     */
    protected abstract String getJdbcDriverClassName();

    /**
     * Retrieve a {@link SqlQueryChannel.Descriptor} for this instance.
     *
     * @return the {@link SqlQueryChannel.Descriptor}
     */
    public SqlQueryChannel.Descriptor getSqlQueryChannelDescriptor() {
        return this.sqlQueryChannelDescriptor;
    }

    /**
     * Creates a new {@link DatabaseDescriptor} for this instance and the given {@link Configuration}.
     *
     * @param configuration provides configuration information for the result
     * @return the {@link DatabaseDescriptor}
     */
    public DatabaseDescriptor createDatabaseDescriptor(Configuration configuration) {
        return new DatabaseDescriptor(
                configuration.getStringProperty(this.jdbcUrlProperty),
                configuration.getStringProperty(this.jdbcUserProperty, null),
                configuration.getStringProperty(this.jdbcPasswordProperty, null),
                this.getJdbcDriverClassName()
        );
    }
}