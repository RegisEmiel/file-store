package com.geekbrains.geekmarketwinter.repositories;

import com.geekbrains.geekmarketwinter.entites.FileMetaDTO;
import com.geekbrains.geekmarketwinter.entites.ProductDTO;
import com.geekbrains.geekmarketwinter.repositories.interfaces.IFileMetaProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import java.util.Collection;
import java.util.UUID;


@Component
public class FileMetaProvider implements IFileMetaProvider {

    private static final String GET_FILES_META = "select hashcode, filename from winter_market.file_info_metadata where sub_type = :subtype";

    private static final String GET_FILE_PATH_BY_HASH = "select filename from winter_market.file_info_metadata where hashcode = :hash";

    private static final String SAVE_FILE_META_DATA = "insert into winter_market.file_info_metadata (hashcode, filename, sub_type)\n" +
            "values (:hash, :finame, :subtype)";

    private static final String GET_FILE_COUNT_BY_NAME = "" +
            "select\n" +
            "\tcount(q.hashcode) as file_count\n" +
            "from winter_market.file_info_metadata as q\n" +
            "inner join ( \n" +
            "select\n" +
            "\thashcode,\n" +
            "\tfilename\n" +
            "from winter_market.file_info_metadata as metadata\n" +
            "where filename = :file_name) as subquery\n" +
            "on q.hashcode = subquery.hashcode;";

    private static final String GET_HASH_BY_FILENAME = "" +
            "select\n" +
            "\thashcode\n" +
            "from winter_market.file_info_metadata\n" +
            "where filename = :file_name\n" +
            "limit 1;";

    private static final String DELETE_BY_FILENAME = "delete from winter_market.file_info_metadata where filename = :file_name;";

    private final Sql2o sql2o;

    public FileMetaProvider(@Autowired Sql2o sql2o) {
        this.sql2o = sql2o;
    }


    @Override
    public String checkFileExists(UUID fileHash) {
        try (Connection connection = sql2o.open()) {
            return connection.createQuery(GET_FILE_PATH_BY_HASH, false)
                    .addParameter("hash", fileHash.toString())
                    .executeScalar(String.class);
        }
    }

    @Override
    public void saveFileMeta(UUID fileHash, String fileName, int sybType) {
        try (Connection connection = sql2o.open()) {
            connection.createQuery(SAVE_FILE_META_DATA)
                    .addParameter("hash", fileHash.toString())
                    .addParameter("finame", fileName)
                    .addParameter("subtype", sybType)
                    .executeUpdate();
        }
    }

    @Override
    public Collection<FileMetaDTO> getMetaFiles(int subtype) {
        try (Connection connection = sql2o.open()) {
            return connection.createQuery(GET_FILES_META, false)
                    .addParameter("subtype", subtype)
                    .executeAndFetch(FileMetaDTO.class);
        }
    }

    @Override
    public int getFileCountByName(String fileName) {
        try (Connection connection = sql2o.open()) {
            return connection.createQuery(GET_FILE_COUNT_BY_NAME, false)
                    .addParameter("file_name", fileName)
                    .executeScalar(int.class);
        }
    }

    @Override
    public String getHashByFileName(String fileName) {
        try (Connection connection = sql2o.open()) {
            return connection.createQuery(GET_HASH_BY_FILENAME, false)
                    .addParameter("file_name", fileName)
                    .executeScalar(String.class);
        }
    }

    @Override
    public void deleteByFileName(String fileName) {
        try (Connection connection = sql2o.open()) {
            connection.createQuery(DELETE_BY_FILENAME, false)
                    .addParameter("file_name", fileName)
                    .executeUpdate();
        }
    }
}