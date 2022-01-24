package com.geekbrains.geekmarketwinter.repositories.interfaces;

import java.io.IOException;
import java.util.UUID;

public interface IFileSystemProvider {

    byte[] getFile(String fileHash) throws IOException;

    String storeFile(byte[] content, UUID md5, String fileName)throws IOException;

    void deleteFile(String fileHash)throws IOException;
}
