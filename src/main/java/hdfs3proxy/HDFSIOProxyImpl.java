package hdfs3proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HDFSIOProxyImpl implements HDFSIOProxy {
    private static final Map<String, DistributedFileSystem> fsCache = new ConcurrentHashMap<>();
    private DistributedFileSystem rawHDFSFS;
    private final ClassLoader currentClassLoader;

    public HDFSIOProxyImpl(String nameNode, String configStr) throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        //在和hdfs集群通信时，替换掉class loader，换成加载HDFSIOProxyImpl的classloader
        currentClassLoader = this.getClass().getClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        //加载默认配置
        Configuration config = new Configuration();
        //增加从外部传来的而外配置信息
        config.addResource(new ByteArrayInputStream(configStr.getBytes()));
        rawHDFSFS = fsCache.get(nameNode);
        if (rawHDFSFS == null) {
            synchronized (HDFSIOProxyImpl.class) {
                rawHDFSFS = fsCache.get(nameNode);
                if (rawHDFSFS == null) {
                    try {
                        //TODO 直接初始化，是否会影响trino中的逻辑。在trino中，通过hdfs protocol返回的不是DistributedFileSystem
                        FileSystem rawFs=new DistributedFileSystem();
                        rawFs.initialize(new URI("hdfs://" + nameNode),config);
                        rawHDFSFS = HDFSIOProxyFactory.unwrapHDFSFileSystem(rawFs);
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                    fsCache.put(nameNode, rawHDFSFS);
                }
            }
        }
        Thread.currentThread().setContextClassLoader(oriClassLoader);
    }

    @Override
    public OutputStream create(String filePath, boolean overwrite, int bufferSize, short rep, long blockSize)
            throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            return rawHDFSFS.create(new Path(filePath), overwrite, bufferSize, rep, blockSize);
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }

    @Override
    public long getFileLength(String filePath) throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            FileStatus status = rawHDFSFS.getFileStatus(new Path(filePath));
            return status.getLen();
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }

    @Override
    public boolean delete(String filePath, boolean recursive) throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            return rawHDFSFS.delete(new Path(filePath), recursive);
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }

    @Override
    public boolean existFile(String filePath) throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            try {
                if (rawHDFSFS.getFileStatus(new Path(filePath)) != null) {
                    return true;
                }
                return false;
            } catch (FileNotFoundException e) {
                return false;
            }
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }

    @Override
    public byte[] getFileChecksum(String filePath) throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            FileChecksum fileChecksum = rawHDFSFS.getFileChecksum(new Path(filePath));
            return fileChecksum.getBytes();
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }

    @Override
    public void mkdirs(String path) throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            rawHDFSFS.mkdirs(new Path(path));
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }

    @Override
    public boolean rename(String srcFilePath, String destFilePath, boolean overwrite) throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            if (overwrite) {
                rawHDFSFS.rename(new Path(srcFilePath), new Path(destFilePath), Rename.OVERWRITE);
                return true;
            } else {
                return rawHDFSFS.rename(new Path(srcFilePath), new Path(destFilePath));
            }
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }

    @Override
    public InputStream open(String filePath) throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            return rawHDFSFS.open(new Path(filePath));
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }


    @Override
    public ProxyFileStatus getFileStatus(String path) throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            FileStatus fileStatus = rawHDFSFS.getFileStatus(new Path(path));
            return convertStatus(fileStatus);
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }

    private ProxyFileStatus convertStatus(FileStatus fileStatus) {
        return new ProxyFileStatus(fileStatus.getLen(), //
                fileStatus.isDir(), //
                fileStatus.getReplication(), //
                fileStatus.getBlockSize(), //
                fileStatus.getModificationTime(), //
                fileStatus.getPath().toUri().getPath());
    }

    @Override
    public ProxyFileStatus[] listStatus(String path) throws IOException {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            FileStatus[] nativeStatuses = rawHDFSFS.listStatus(new Path(path));
            ProxyFileStatus[] result = new ProxyFileStatus[nativeStatuses.length];
            for (int i = 0; i < nativeStatuses.length; i++) {
                result[i] = convertStatus(nativeStatuses[i]);
            }
            return result;
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }

    @Override
    public String getTrashRootPath(String path) {
        ClassLoader oriClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        try {
            return rawHDFSFS.getTrashRoot(new Path(path)).toUri().getPath();
        } finally {
            Thread.currentThread().setContextClassLoader(oriClassLoader);
        }
    }

}
