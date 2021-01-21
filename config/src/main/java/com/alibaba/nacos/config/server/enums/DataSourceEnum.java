/*
 * Copyright (C), 2008-2021, paraview
 */

package com.alibaba.nacos.config.server.enums;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * common database enum.
 * references :springboot.jdbc.DatabaseDriver.
 *
 * @author syoka
 */
public enum DataSourceEnum {


    /**
     * UNKNOW.
     */
    UNKNOWN(null, null),

    /**
     * Apache Derby.
     */
    DERBY("DERBY", "org.apache.derby.jdbc.EmbeddedDriver", "org.apache.derby.jdbc.EmbeddedXADataSource",
        "SELECT 1 FROM SYSIBM.SYSDUMMY1"),

    /**
     * H2.
     */
    H2("H2", "org.h2.Driver", "org.h2.jdbcx.JdbcDataSource", "SELECT 1"),

    /**
     * HyperSQL DataBase.
     */
    HSQLDB("HSQLDB", "org.hsqldb.jdbc.JDBCDriver", "org.hsqldb.jdbc.pool.JDBCXADataSource",
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SYSTEM_USERS"),

    /**
     * SQL Lite.
     */
    SQLITE("SQLITE", "org.sqlite.JDBC"),

    /**
     * MySQL.
     */
    MYSQL("MYSQL", "com.mysql.cj.jdbc.Driver", "com.mysql.cj.jdbc.MysqlXADataSource", "/* ping */ SELECT 1"),

    /**
     * Maria DB.
     */
    MARIADB("MARIADB", "org.mariadb.jdbc.Driver", "org.mariadb.jdbc.MariaDbDataSource", "SELECT 1") {
        @Override
        public String getId() {
            return "mysql";
        }
    },

    /**
     * Google App Engine.
     */
    GAE(null, "com.google.appengine.api.rdbms.AppEngineDriver"),

    /**
     * Oracle.
     */
    ORACLE("ORACLE", "oracle.jdbc.OracleDriver", "oracle.jdbc.xa.client.OracleXADataSource",
        "SELECT 'Hello' from DUAL"),

    /**
     * Postgres.
     */
    POSTGRESQL("POSTGRESQL", "org.postgresql.Driver", "org.postgresql.xa.PGXADataSource", "SELECT 1"),

    /**
     * HANA - SAP HANA Database - HDB.
     *
     * @since 2.1.0
     */
    HANA("HDB", "com.sap.db.jdbc.Driver", "com.sap.db.jdbcext.XADataSourceSAP", "SELECT 1 FROM SYS.DUMMY") {
        @Override
        protected Collection<String> getUrlPrefixes() {
            return Collections.singleton("sap");
        }
    },

    /**
     * jTDS. As it can be used for several databases, there isn't a single product name we
     * could rely on.
     */
    JTDS(null, "net.sourceforge.jtds.jdbc.Driver"),

    /**
     * SQL Server.
     */
    SQLSERVER("SQLSERVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "com.microsoft.sqlserver.jdbc.SQLServerXADataSource", "SELECT 1") {
        @Override
        protected boolean matchProductName(String productName) {
            return super.matchProductName(productName) || "SQL SERVER".equalsIgnoreCase(productName);

        }

    },

    /**
     * Firebird.
     */
    FIREBIRD("FIREBIRD", "org.firebirdsql.jdbc.FBDriver", "org.firebirdsql.ds.FBXADataSource",
        "SELECT 1 FROM RDB$DATABASE") {
        @Override
        protected Collection<String> getUrlPrefixes() {
            return Collections.singleton("firebirdsql");
        }

        @Override
        protected boolean matchProductName(String productName) {
            return super.matchProductName(productName)
                || productName.toLowerCase(Locale.ENGLISH).startsWith("firebird");
        }
    },

    /**
     * DB2 Server.
     */
    DB2("DB2", "com.ibm.db2.jcc.DB2Driver", "com.ibm.db2.jcc.DB2XADataSource", "SELECT 1 FROM SYSIBM.SYSDUMMY1") {
        @Override
        protected boolean matchProductName(String productName) {
            return super.matchProductName(productName) || productName.toLowerCase(Locale.ENGLISH).startsWith("db2/");
        }
    },

    /**
     * DB2 AS400 Server.
     */
    DB2_AS400("DB2_AS400", "com.ibm.as400.access.AS400JDBCDriver",
        "com.ibm.as400.access.AS400JDBCXADataSource", "SELECT 1 FROM SYSIBM.SYSDUMMY1") {
        @Override
        public String getId() {
            return "db2";
        }

        @Override
        protected Collection<String> getUrlPrefixes() {
            return Collections.singleton("as400");
        }

        @Override
        protected boolean matchProductName(String productName) {
            return super.matchProductName(productName) || productName.toLowerCase(Locale.ENGLISH).contains("as/400");
        }
    },

    /**
     * Teradata.
     */
    TERADATA("TERADATA", "com.teradata.jdbc.TeraDriver"),

    /**
     * Informix.
     */
    INFORMIX("INFORMIX", "com.informix.jdbc.IfxDriver", null, "select count(*) from systables") {
        @Override
        protected Collection<String> getUrlPrefixes() {
            return Arrays.asList("informix-sqli", "informix-direct");
        }

    },

    /**
     * DAMENG.
     */
    DAMENG("DAMENG", "dm.jdbc.driver.DmDriver", null, "select 1") {
    };

    private final String productName;

    private final String driverClassName;

    private final String xaDataSourceClassName;

    private final String validationQuery;

    DataSourceEnum(String productName, String driverClassName) {
        this(productName, driverClassName, null);
    }

    DataSourceEnum(String productName, String driverClassName, String xaDataSourceClassName) {
        this(productName, driverClassName, xaDataSourceClassName, null);
    }

    DataSourceEnum(String productName, String driverClassName, String xaDataSourceClassName, String validationQuery) {
        this.productName = productName;
        this.driverClassName = driverClassName;
        this.xaDataSourceClassName = xaDataSourceClassName;
        this.validationQuery = validationQuery;
    }

    /**
     * Return the identifier of this driver.
     *
     * @return the identifier
     */
    public String getId() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    protected boolean matchProductName(String productName) {
        return this.productName != null && this.productName.equalsIgnoreCase(productName);
    }

    protected Collection<String> getUrlPrefixes() {
        return Collections.singleton(this.name().toLowerCase(Locale.ENGLISH));
    }

    /**
     * Return the driver class name.
     *
     * @return the class name or {@code null}
     */
    public String getDriverClassName() {
        return this.driverClassName;
    }

    /**
     * Return the XA driver source class name.
     *
     * @return the class name or {@code null}
     */
    public String getXaDataSourceClassName() {
        return this.xaDataSourceClassName;
    }

    /**
     * Return the validation query.
     *
     * @return the validation query or {@code null}
     */
    public String getValidationQuery() {
        return this.validationQuery;
    }

    /**
     * Find a {@link DataSourceEnum} for the given URL.
     *
     * @param url the JDBC URL
     * @return the database driver or {@link #UNKNOWN} if not found
     */
    public static DataSourceEnum fromJdbcUrl(String url) {
        if (StringUtils.hasLength(url)) {
            Assert.isTrue(url.startsWith("jdbc"), "URL must start with 'jdbc'");
            String urlWithoutPrefix = url.substring("jdbc".length()).toLowerCase(Locale.ENGLISH);
            for (DataSourceEnum driver : values()) {
                for (String urlPrefix : driver.getUrlPrefixes()) {
                    String prefix = ":" + urlPrefix + ":";
                    if (driver != UNKNOWN && urlWithoutPrefix.startsWith(prefix)) {
                        return driver;
                    }
                }
            }
        }
        return UNKNOWN;
    }

    /**
     * Find a {@link DataSourceEnum} for the given product name.
     *
     * @param productName product name
     * @return the database driver or {@link #UNKNOWN} if not found
     */
    public static DataSourceEnum fromProductName(String productName) {
        if (StringUtils.hasLength(productName)) {
            productName = productName.toUpperCase();
            for (DataSourceEnum candidate : values()) {
                if (candidate.matchProductName(productName)) {
                    return candidate;
                }
            }
        }
        return UNKNOWN;
    }


    /**
     * inner datasource name
     */
    public static class DataSourceName {
        public static final String UNKNOWN = "UNKNOWN";
        public static final String DERBY = "DERBY";
        public static final String H2 = "H2";
        public static final String HSQLDB = "HSQLDB";
        public static final String SQLITE = "SQLITE";
        public static final String MYSQL = "MYSQL";
        public static final String MARIADB = "MARIADB";
        public static final String GAE = "GAE";
        public static final String ORACLE = "ORACLE";
        public static final String POSTGRESQL = "POSTGRESQL";
        public static final String HANA = "HANA";
        public static final String JTDS = "JTDS";
        public static final String SQLSERVER = "SQLSERVER";
        public static final String DB2_AS400 = "DB2_AS400";
        public static final String TERADATA = "TERADATA";
        public static final String INFORMIX = "INFORMIX";
        public static final String DAMENG = "DAMENG";
    }

}
