package com.walletledger.ledger;

import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Every service reads the same DATABASE_URL, in the form
 * postgres://user:password@host:5432/wallet?sslmode=disable
 * Java's driver wants a JDBC URL plus a separate user and password, so we convert it here.
 */
@Configuration
class DataSourceConfig {

    @Bean
    DataSource dataSource(@Value("${DATABASE_URL}") String databaseUrl,
                          @Value("${DB_POOL_MAX:10}") int poolMax) {
        URI uri = URI.create(databaseUrl);
        String[] userInfo = uri.getUserInfo().split(":", 2);
        int port = uri.getPort() == -1 ? 5432 : uri.getPort();
        String query = uri.getQuery() == null ? "" : "?" + uri.getQuery();

        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("jdbc:postgresql://" + uri.getHost() + ":" + port + uri.getPath() + query);
        ds.setUsername(URLDecoder.decode(userInfo[0], StandardCharsets.UTF_8));
        ds.setPassword(userInfo.length > 1 ? URLDecoder.decode(userInfo[1], StandardCharsets.UTF_8) : "");
        ds.setMaximumPoolSize(poolMax);
        ds.setPoolName("ledger");
        return ds;
    }
}