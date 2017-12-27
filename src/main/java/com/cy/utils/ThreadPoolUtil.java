package com.cy.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * 线程池工具类
 * Created by cy on 2017/12/27 23:31.
 */
public class ThreadPoolUtil {
    private static Logger log = LoggerFactory.getLogger(ThreadPoolUtil.class);
    private static final ThreadPoolUtil threadPoolUtil = new ThreadPoolUtil();
    private ThreadPoolExecutor executor = null;
    //TODO 线程池基础参数，后期写入配置文件中
    private int corePoolSize = 10;//核心池的大小 运行线程的最大值 当线程池中的线程数目达到corePoolSize后，就会把多余的任务放到缓存队列当中
    private int maxNumPoolSize = 15;//创建线程最大值
    private long keepAliveTime = 1;//线程没有执行任务时 被保留的最长时间 超过这个时间就会被销毁 直到线程数等于 corePoolSize
    private long timeout = 10;//等待线程池任务执行结束超时时间
    /**    参数keepAliveTime的时间单位，有7种取值，在TimeUnit类中有7种静态属性：
     TimeUnit.DAYS;               天
     TimeUnit.HOURS;             小时
     TimeUnit.MINUTES;           分钟
     TimeUnit.SECONDS;           秒
     TimeUnit.MILLISECONDS;      毫秒
     TimeUnit.MICROSECONDS;      微妙
     TimeUnit.NANOSECONDS;       纳秒***/
    private TimeUnit unit = TimeUnit.SECONDS;
    /**
     * 用来存储等待中的任务容器
     * ArrayBlockingQueue;
     * LinkedBlockingQueue;
     * SynchronousQueue;
     */
    private LinkedBlockingQueue workQueue = new LinkedBlockingQueue<Runnable>();
    public static ThreadPoolUtil getThreadPoolUtil() {
        return threadPoolUtil;
    }
    private ThreadPoolUtil(){
        //实现线程池
        executor = new ThreadPoolExecutor(corePoolSize,maxNumPoolSize,keepAliveTime,unit,workQueue);
        log.info("线程池初始化成功");
    }

    /**
     * 获取线程池
     * @return
     */
    public ThreadPoolExecutor getExecutor(){
        return executor;
    }

    /**
     * 准备执行
     * @param t
     */
    public void execute(Thread t){
        executor.execute(t);
    }
    public void execute(Runnable r){
        executor.execute(r);
    }
    /**
     *
     * @return
     */
    public int getQueueSize(){
        return executor.getQueue().size();
    }

    /**
     * 异步提交返回Future
     * Future.get()可获得返回结果
     * @param r
     * @return
     */
    public Future<?> submit(Runnable r) {
        return executor.submit(r);
    }
    public Future<?> submit(Callable t){
        return getExecutor().submit(t);
    }

    /**
     * 销毁线程池
     */
    public void destory(){
        getExecutor().shutdown();
    }
    /**
     * 阻塞，直到线程池里所有任务结束
     */
    public void awaitTermination() throws InterruptedException {
        log.info("Thread pool ,awaitTermination started, please wait till all the jobs complete.");
        executor.awaitTermination(timeout,unit);
    }
}
