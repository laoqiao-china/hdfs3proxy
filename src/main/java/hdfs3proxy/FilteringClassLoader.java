package hdfs3proxy;

import org.apache.commons.lang.StringUtils;

import java.net.URL;
import java.net.URLClassLoader;

public class FilteringClassLoader extends URLClassLoader {

    private final ClassLoader parent;

    public FilteringClassLoader(URL[] urls, ClassLoader parent) {
        super(urls);
        this.parent = parent;
    }


    @Override
    public URL getResource(String name) {
        URL url = findResource(name);
        if (url == null) {
            if (getParent() != null) {
                url = getParent().getResource(name);
            }
        }
        return url;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            //hdfs jar中只包含hadoop相关的class，所以如果其他class，需要使用parent的class加载
            Class<?> r = findLoadedClass(name);
            if (r != null) {
                return r;
            }
            if (StringUtils.startsWith(name, "org.apache.hadoop")) {
                throw new ClassNotFoundException(String.format("%s 没有在hdfs3.jar中找到，你重新为其打包",name));
            }
            // HDFSIOProxyImpl需要打包到hdfs3.jar中，这样在HDFSIOProxyImpl才可以使用this.getClass().getClassLoader()得到本classloader
            //也就是说HDFSIOProxyImpl和hdfs3的class，共用一个classloader
            if (StringUtils.equals(name, "hdfs3proxy.HDFSIOProxyImpl")) {
                return super.findClass(name);
            }
            return parent.loadClass(name);
        }
    }

}
