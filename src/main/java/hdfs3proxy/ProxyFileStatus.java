package hdfs3proxy;

public class ProxyFileStatus {
    private final long len;
    private final boolean dir;
    private final short replication;
    private final long blockSize;
    private final long modificationTime;
    private final String path;

    public ProxyFileStatus(long len, boolean dir, short replication, long blockSize, long modificationTime, String path) {
        this.len=len;
        this.dir=dir;
        this.replication=replication;
        this.blockSize=blockSize;
        this.modificationTime=modificationTime;
        this.path=path;
    }
}
