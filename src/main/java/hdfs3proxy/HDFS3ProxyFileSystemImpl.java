package hdfs3proxy;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class HDFS3ProxyFileSystemImpl extends FileSystem {

    @Override
    public URI getUri() {
        return null;
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        //从Path中得到namenode名字
        String nameNode=path.toUri().getHost();
        HDFSIOProxy ioProxy = HDFSIOProxyFactory.getHDFSProxy(nameNode, getConf(), HDFSIOProxyFactory.isHdfsVersion3(nameNode));
        return new FSDataInputStream(ioProxy.open(path.toString()));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable) throws IOException {
        return null;
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        return null;
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        return false;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        return false;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path) {

    }

    @Override
    public Path getWorkingDirectory() {
        return null;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return null;
    }
}
