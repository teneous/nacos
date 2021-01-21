/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.alibaba.nacos.config.server.service.datasource;

import com.alibaba.nacos.config.server.enums.DataSourceEnum;
import com.alibaba.nacos.config.server.utils.PropertyUtil;
import com.google.common.base.Preconditions;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.util.Strings;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.alibaba.nacos.common.utils.CollectionUtils.getOrDefault;

/**
 * Properties of external DataSource.
 *
 * @author Nacos
 */
public class ExternalDataSourceProperties {

    private Integer num;

    private List<String> url = new ArrayList<>();

    private List<String> user = new ArrayList<>();

    private List<String> password = new ArrayList<>();

    public void setNum(Integer num) {
        this.num = num;
    }

    public void setUrl(List<String> url) {
        this.url = url;
    }

    public void setUser(List<String> user) {
        this.user = user;
    }

    public void setPassword(List<String> password) {
        this.password = password;
    }

    /**
     * Build serveral HikariDataSource.
     *
     * @param environment {@link Environment}
     * @param callback Callback function when constructing data source
     * @return List of {@link HikariDataSource}
     */
    List<HikariDataSource> build(Environment environment, Callback<HikariDataSource> callback) {
        List<HikariDataSource> dataSources = new ArrayList<>();
        Binder.get(environment).bind("db", Bindable.ofInstance(this));
        Preconditions.checkArgument(Objects.nonNull(num), "db.num is null");
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(user), "db.user or db.user.[index] is null");
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(password), "db.password or db.password.[index] is null");
        String externalStorage = PropertyUtil.getExternalStorage();
        Preconditions.checkArgument(Strings.isNotEmpty(externalStorage), "db.platform is null");

        for (int index = 0; index < num; index++) {
            int currentSize = index + 1;
            Preconditions.checkArgument(url.size() >= currentSize, "db.url.%s is null", index);
            DataSourcePoolProperties poolProperties = DataSourcePoolProperties.build(environment);

            poolProperties.setDriverClassName(getDriverClassName(externalStorage));
            poolProperties.setJdbcUrl(url.get(index).trim());
            poolProperties.setUsername(getOrDefault(user, index, user.get(0)).trim());
            poolProperties.setPassword(getOrDefault(password, index, password.get(0)).trim());
            HikariDataSource ds = poolProperties.getDataSource();
            ds.setConnectionTestQuery(DataSourceEnum.MYSQL.getValidationQuery());
            dataSources.add(ds);
            callback.accept(ds);
        }
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(dataSources), "no datasource available");
        return dataSources;
    }

    interface Callback<D> {

        /**
         * Perform custom logic.
         *
         * @param datasource dataSource.
         */
        void accept(D datasource);
    }


    /**
     * return classDriverName through DataSourceEnum
     * special ops for mysql, cuz mysql has different driver class path,we use Class.forName to detect mysql version
     *
     * <li>com.mysql.cj.jdbc.Driver</>
     * <li>com.mysql.jdbc.Driver</>
     *
     * @return com.mysql.cj.jdbc.Driver or com.mysql.jdbc.Driver
     */
    private String getDriverClassName(String storageName) {
        DataSourceEnum dataSourceEnum = DataSourceEnum.fromProductName(storageName);
        if (dataSourceEnum == null) {
            return null;
        }

        if (DataSourceEnum.MYSQL == dataSourceEnum) {
            String driverClassName = DataSourceEnum.MYSQL.getDriverClassName();
            String[] driverClazz = driverClassName.split(",");
            for (String clazz : driverClazz) {
                try {
                    Class.forName(driverClassName);
                    return clazz;
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        } else {
            return dataSourceEnum.getDriverClassName();
        }
        return null;
    }
}
