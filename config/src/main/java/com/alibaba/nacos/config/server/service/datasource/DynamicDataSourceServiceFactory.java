/*
 * Copyright (C), 2008-2021, paraview
 */
package com.alibaba.nacos.config.server.service.datasource;

import com.alibaba.nacos.config.server.enums.DataSourceEnum;

/**
 * Datasource Factory
 *
 * @author syoka
 */
public class DynamicDataSourceServiceFactory {

    /**
     * return specify DataSourceService through dbName {@link DataSourceService}
     *
     * @param dbName 数据库类型
     * @return DataSourceService
     */
    public DataSourceService selectDataSource(String dbName) {
        dbName = dbName.toUpperCase();
        switch (dbName) {
            //you can extends your scene
            case DataSourceEnum.DataSourceName.DAMENG:
                return new DmDataSourceServiceImpl();
            case DataSourceEnum.DataSourceName.ORACLE:
                return new OracleDataSourceServiceImpl();
            case DataSourceEnum.DataSourceName.MYSQL:
            default:
                return new MysqlDataSourceServiceImpl();
        }
    }
}
