package com.memgres.junit5;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * Minimal {@link DataSource} implementation backed by a Memgres instance.
 * Suitable for passing to frameworks that expect a DataSource (Spring, ORM frameworks, SQL builders, etc.).
 */
class MemgresDataSource implements DataSource {

    private final MemgresExtension extension;
    private PrintWriter logWriter;
    private int loginTimeout;

    MemgresDataSource(MemgresExtension extension) {
        this.extension = extension;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return extension.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return extension.getConnection();
    }

    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() {
        return loginTimeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }
}
