package com.geekbrains.geekmarketwinter.repositories;

import com.geekbrains.geekmarketwinter.entites.ProductDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import java.util.Collection;

@Component
public class TestRepository {
    private final Sql2o sql2o;
    private static final String SELECT_PRODUCTS_QUERY = "select hash, file_size as fileSize from file_metadata";

    public TestRepository(@Autowired Sql2o sql2o) {
        this.sql2o = sql2o;
    }

    public Collection<ProductDTO> getProducts(int id) {
        try (Connection connection = sql2o.open()) {
            return connection.createQuery(SELECT_PRODUCTS_QUERY, false)
                    .executeAndFetch(ProductDTO.class);
        }
    }
}
