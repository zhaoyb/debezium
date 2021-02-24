/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Strings;

/**
 *
 * 这个类算是入口方法了，debezium 在使用的时候， 需要注册到 kafka Connect
 *
 *  kafka Connect ，我目前还不是特别了解， 这里简单说一下：
 *  kafka Connect 可以算是对 kafka 的再次包装，
 *
 *
 *  举个例子，如果你需要把你的数据库中的数据，写入到 kafka， 你可能需要循环数据库， 查找数据， 然后连接 kafka， 最后发送数据。
 *
 *  这里你不仅要关注数据本身，还需要关注连接 kafka 和发送数据的细节。 而连接 kafka 和 发送数据 其实和业务没有多大关系。
 *
 *  kafka Connect就是帮你连接 kafka 和发送数据的。
 *
 *  但是如果kafka Connect仅做了上面的工作，其实作用并不大， 无外乎你现在需要连接 kafka connect
 *
 *  kafka connect最神奇的地方就是它可以主动调用你的产生数据的类
 *
 *
 *  你需要将你自己的类注册到kafka Connect中去， 之后kafka Connect会启动运行你的类， 而你的类会产生数据，kafka Connect会把数据会被送入到 kafka中去。
 *  使用kafka Connect 的好处就是你不再需要关注 如何连接kafka， 如何发送 kafka 数据等细节，只需要关注数据本身。
 *
 *  具体实现上，如果要注册自己的类进到kafka Connect，能让kafka Connect识别并且运行， 就需要继承或者实现kafka Connect指定的类或者接口
 *
 *  下面继承的 'SourceConnector' 就是kafka Connect中的概念了，具体可以参考kafka Connect
 *
 *
 *
 *
 * A Kafka Connect source connector that creates tasks that read the MySQL binary log and generate the corresponding
 * data change events.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link MySqlConnectorConfig}.
 *
 *
 * @author Randall Hauch
 */
public class MySqlConnector extends SourceConnector {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, String> props;

    public MySqlConnector() {
    }

    /**
     * 版本号，kafka Connect中的概念
     *
     * @return
     */
    @Override
    public String version() {
        return Module.version();
    }

    /**
     *
     * 这里需要返回 task 类， 这个 task 类是 kafka Connect中的概念
     *
     * @return
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MySqlConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return props == null ? Collections.emptyList() : Collections.singletonList(new HashMap<String, String>(props));
    }

    @Override
    public void stop() {
        this.props = null;
    }

    @Override
    public ConfigDef config() {
        return MySqlConnectorConfig.configDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Configuration config = Configuration.from(connectorConfigs);

        // First, validate all of the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = config.validate(MySqlConnectorConfig.EXPOSED_FIELDS);

        // Get the config values for each of the connection-related fields ...
        ConfigValue hostnameValue = results.get(MySqlConnectorConfig.HOSTNAME.name());
        ConfigValue portValue = results.get(MySqlConnectorConfig.PORT.name());
        ConfigValue userValue = results.get(MySqlConnectorConfig.USER.name());
        final String passwordValue = config.getString(MySqlConnectorConfig.PASSWORD);

        if (Strings.isNullOrEmpty(passwordValue)) {
            logger.warn("The connection password is empty");
        }

        // If there are no errors on any of these ...
        if (hostnameValue.errorMessages().isEmpty()
                && portValue.errorMessages().isEmpty()
                && userValue.errorMessages().isEmpty()) {
            // Try to connect to the database ...
            try (MySqlJdbcContext jdbcContext = new MySqlJdbcContext(new MySqlConnectorConfig(config))) {
                jdbcContext.start();
                JdbcConnection mysql = jdbcContext.jdbc();
                try {
                    mysql.execute("SELECT version()");
                    logger.info("Successfully tested connection for {} with user '{}'", jdbcContext.connectionString(), mysql.username());
                }
                catch (SQLException e) {
                    logger.info("Failed testing connection for {} with user '{}'", jdbcContext.connectionString(), mysql.username());
                    hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
                }
                finally {
                    jdbcContext.shutdown();
                }
            }
        }
        return new Config(new ArrayList<>(results.values()));
    }
}
