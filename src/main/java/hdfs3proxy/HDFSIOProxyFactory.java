package hdfs3proxy;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;

public class HDFSIOProxyFactory {

    private static Logger LOG = LoggerFactory.getLogger(HDFSIOProxyFactory.class);

    public static HDFSIOProxy getHDFSProxy(String nameNode, Configuration config, boolean hdfs3)
            throws IOException {
        if (!hdfs3) {
            StringWriter configStr = new StringWriter();
            config.writeXml(configStr);
            //不是hdfs3，直接用系统默认的classloader初始化
            return new HDFSIOProxyImpl(nameNode, configStr.getBuffer().toString());
        } else {
            FilteringClassLoader hdfs3ClassLoader = initClassLoaderForLoadHdfs(config,Thread.currentThread().getContextClassLoader());
            try {
                Class<HDFSIOProxy> classHDFS3 = (Class<HDFSIOProxy>) hdfs3ClassLoader.loadClass(HDFSIOProxyImpl.class.getName());
                Constructor<HDFSIOProxy> constructor = classHDFS3
                        .getConstructor(new Class[]{String.class, String.class});
                StringWriter configStr = new StringWriter();
                //TODO hdfs3需要手动设置hdfs的映射(否则在presto里面会出问题，但奇怪的是hive不会。。。。)
                config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
                config.writeXml(configStr);
                LOG.info("init hdfs {} extend store use proxyclassloader,native namenode {}", hdfs3, nameNode);
                return constructor
                        .newInstance(new Object[]{nameNode, configStr.getBuffer().toString()});
            } catch (ClassNotFoundException | NoClassDefFoundError | InstantiationException | IllegalAccessException
                    | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
                    | SecurityException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static DistributedFileSystem unwrapHDFSFileSystem(FileSystem rawFs){
        if (rawFs instanceof DistributedFileSystem) {
            return (DistributedFileSystem) rawFs;
        } else if (rawFs instanceof FilterFileSystem filterRawFs) {// 针对trino
            rawFs = filterRawFs.getRawFileSystem();
            return(DistributedFileSystem) rawFs;
        } else {
            throw new RuntimeException(String.format("不支持将%s转换为DistributedFileSystem",rawFs.getClass().getName()));
        }
    }

    public static boolean isHdfsVersion3(String nameNode) {
//        DistributedFileSystem nativeHDFSFS = HDFSIOProxyFactory.unwrapHDFSFileSystem(rawFs);
//        ClientProtocol nameNode = nativeHDFSFS.getClient().getNamenode();
        //TODO 怎么得到nn的hdfs版本？ClientProtocol居然没有返回nn版本号的接口
        if(StringUtils.equalsIgnoreCase("hdfs3",nameNode)){
            return true;
        }
        return false;
    }

    private static FilteringClassLoader initClassLoaderForLoadHdfs(Configuration config, ClassLoader ctxClassLoader) throws IOException {
        //通过系统参数或者config将hdfs3的jar传入进去
        String hdfsClassPath = System.getProperty("HDFS3_CP");
        if (StringUtils.isEmpty(hdfsClassPath)) {
            hdfsClassPath = config.get("hdfsproxy.hdfs3jar");
            if (StringUtils.isEmpty(hdfsClassPath)) {
                throw new RuntimeException(
                        "需要使用-DHDFS3_CP或配置文件中使用'hdfsproxy.hdfs3jar'指定hdfs3的包");
            }
        }
        String hdfs3Path = "file:" + hdfsClassPath;
        URL hdfs3ClassPath = new URL(hdfs3Path);
        File file = new File(hdfs3Path);
        if (file.length() < 0) {
            throw new RuntimeException("hdfs3 file is not valid!");
        }
        InputStream is = hdfs3ClassPath.openStream();
        if (is == null) {
            throw new RuntimeException("找不到hdfs3ClassPath");
        } else {
            LOG.info("load hdfs3 jar from " + hdfsClassPath);
        }
        is.close();
        return new FilteringClassLoader(new URL[]{hdfs3ClassPath},
                ctxClassLoader);
    }
}
