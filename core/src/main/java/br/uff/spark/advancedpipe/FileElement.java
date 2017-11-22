package br.uff.spark.advancedpipe;

import java.util.Arrays;

/**
 * @author Thaylon Guedes Santos
 * @email thaylongs@gmail.com
 */
public class FileElement {

    private String filePath;
    private String fileName;
    private long fileSize;
    private boolean modified = false;
    private byte[] contents = null;

    public FileElement(String filePath, String fileName, long fileSize, byte[] contents) {
        if (fileName == null)
            throw new NullPointerException("The file name is  null");
        if (fileName.trim().isEmpty())
            throw new IllegalArgumentException("The file name can't be empty");
        this.fileName = fileName;
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.contents = contents;
    }

    @Override
    public String toString() {
        return "FileElement{" +
                "filePath='" + filePath + '\'' +
                ", fileName='" + fileName + '\'' +
                ", fileSize=" + fileSize +
                ", modified=" + modified +
                '}';
    }

    public FileElement copy() {
        return new FileElement(filePath, fileName, fileSize, Arrays.copyOf(contents, contents.length));
    }

    public String getFileName() {
        return fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public byte[] getContents() {
        return contents;
    }

    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }
}
