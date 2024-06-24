package hdfs3proxy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 用来隔离fs sdk环境，hdfs dfsclient之间hdfs的环境，不同层，可以使用不同的hadoop class。该接口中只能出现java sdk中的class
 */
public interface HDFSIOProxy {

	public OutputStream create(String path, boolean b, int bufferSize, short rep, long blockSize) throws IOException;

	public long getFileLength(String path) throws IOException;

	public boolean delete(String filePath, boolean b) throws IOException;

	public boolean existFile(String filePath) throws IOException;

	public byte[] getFileChecksum(String filePathWithNodeId) throws FileNotFoundException, IOException;

	public void mkdirs(String path) throws IOException;

	public boolean rename(String srcFilePath, String destFilePath, boolean overwrite) throws IOException;

	public InputStream open(String filePath) throws IOException;

	public ProxyFileStatus getFileStatus(String path) throws IOException;

	public ProxyFileStatus[] listStatus(String path) throws IOException;

	public String getTrashRootPath(String path);

}
