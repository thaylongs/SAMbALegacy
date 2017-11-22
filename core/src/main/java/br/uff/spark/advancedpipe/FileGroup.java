package br.uff.spark.advancedpipe;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;

import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * @author Thaylon Guedes Santos
 * @email thaylongs@gmail.com
 */
public class FileGroup implements Serializable {

    private Map<String, Object> extraInfo;
    private FileElement[] fileElements;

    public FileGroup(Map<String, Object> extraInfo, FileElement[] fileElements) {
        this.extraInfo = extraInfo;
        this.fileElements = fileElements;
    }

    public Map<String, ? extends Object> getExtraInfo() {
        return extraInfo;
    }

    public FileElement[] getFileElements() {
        return fileElements;
    }

    public void setExtraInfo(Map<String, ? extends Object> extraInfo) {
        this.extraInfo = (Map<String, Object>) extraInfo;
    }

    /* Utils Functions */
    public void saveFilesAt(File dir) throws IOException {
        for (FileElement fileElement : fileElements) {
            File targetFolder = new File(dir, fileElement.getFilePath());
            targetFolder.mkdirs();
            Files.write(new File(targetFolder, fileElement.getFileName()).toPath(), fileElement.getContents());
        }
    }

    /* Builder Functions */
    public static FileGroup of(String filePath, String fileName, byte[] content) {
        return of(filePath, fileName, content, Collections.emptyMap());
    }

    public static FileGroup of(String filePath, String fileName, byte[] content, scala.collection.Map<String, ? extends Object> extraInfo) {
        Map<String, ? extends Object> _extraInfo;

        if (extraInfo.isEmpty())
            _extraInfo = Collections.emptyMap();
        else
            _extraInfo = new HashMap<>(mapAsJavaMapConverter(extraInfo).asJava());

        return of(filePath, fileName, content, (Map<String, Object>) _extraInfo);
    }

    public static FileGroup of(String filePath, String fileName, byte[] content, Map<String, Object> extraInfo) {
        FileElement[] fileElement = new FileElement[]{
                new FileElement(filePath, fileName, content.length, content)
        };
        return new FileGroup((Map<String, Object>) extraInfo, fileElement);
    }

    public static FileGroup of(List<FileElement> result, boolean stillModified) {
        for (FileElement fileElement : result) {
            fileElement.setModified(stillModified);
        }
        return new FileGroup(null, result.toArray(new FileElement[result.size()]));
    }

    public static FileGroup fileGroupOf(File baseDir, Map<String, ? extends Object> extraInfo, Tuple2<String, PortableDataStream>[] data) {
        FileElement[] fileElements = new FileElement[data.length];
        for (int i = 0; i < data.length; i++) {
            Tuple2<String, PortableDataStream> element = data[i];
            byte[] content = element._2.toArray();
            File file = new File(element._1.split(":")[1]);
            String filePath;
            if (baseDir != null) {
                Path parentPath = baseDir.toPath().toAbsolutePath()
                        .relativize(file.toPath())
                        .getParent();
                filePath = parentPath != null ? parentPath.toString() : "";
            } else {
                filePath = file.getParent();
            }
            String fileName = file.getName();
            fileElements[i] = new FileElement(filePath, fileName, content.length, content);
        }
        FileGroup result = new FileGroup((Map<String, Object>) extraInfo, fileElements);
        return result;
    }

    @Override
    public String toString() {
        return "FileGroup{" +
                "extraInfo=" + extraInfo +
                ", fileElements=" + Arrays.toString(fileElements) +
                '}';
    }

}