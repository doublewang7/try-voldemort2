package com.cisco.wap.jdbc;

import com.google.common.collect.Sets;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Set;

public class Driver extends UnregisteredDriver {
    public static final String CONNECT_STRING_PREFIX = "jdbc:voldemort:";
    public static final Set<String> CLIENT_CALCITE_PROP_NAMES = Sets.newHashSet(
            "caseSensitive",
            "unquotedCasing",
            "quoting",
            "conformance"
    );

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            throw new RuntimeException(
                    String.format("Error occurred while registering JDBC driver %s.",
                            Driver.class.getName()),
                    e);
        }
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return DriverVersion.load(Driver.class,
                "voldemort-jdbc.properties",
                "Voldemort JDBC Driver",
                "1.0",
                "Voldemort",
                "1.0");
    }

    @Override
    protected String getFactoryClassName(JdbcVersion jdbcVersion) {
        switch (jdbcVersion) {
            case JDBC_30:
            case JDBC_UNKNOWN:
                throw new UnsupportedOperationException();
            case JDBC_40:
                return VoldemortJdbcFactory.Version40.class.getName();
            case JDBC_41:
            default:
                return VoldemortJdbcFactory.Version41.class.getName();
        }
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    @Override
    public Meta createMeta(AvaticaConnection avaticaConnection) {
        return new VoldemortMeta((VoldemortConnection) avaticaConnection);
    }
}
