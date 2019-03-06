package com.flume.source;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
//import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: com.flume.source
 * @Description: 自定义source，记录偏移量
 * @Date: 2019/03/06 16:27
 * @Version:
 */

//TODO 切记:开发时引入依赖，打包是不包含依赖的

public class MyFlumeSource extends AbstractSource implements Configurable, EventDrivenSource {

    //记录日志
    private static final Logger logger = LoggerFactory.getLogger(MyFlumeSource.class);

    //数据源的文件
    private String filePath;

    //保存Offset偏移量的文件
    private String positionFile;

    //等待时长
    private Long interval;

    //编码格式
    private String charset;

    private FileRunnable fileRunnable;

    private ExecutorService pool;


    /**
     * 读取配置文件（flume在执行一次job时定义的配置文件）
     * 初始化Flume配置信息
     *
     * @param context
     */

    @Override
    public void configure(Context context) {

        //读取哪个文件
        filePath = context.getString("filePath");

        //把Offset偏移量写到哪
        positionFile = context.getString("positionFile");

        //TODO 指定默认每个2秒 去查看一次是否有新的内容
        interval = context.getLong("interval", 2000L);

        //默认使用utf-8
        charset = context.getString("charset", "UTF-8");
    }

    /**
     * 1、创建一个线程来监听一个文件
     */

    @Override
    public synchronized void start() {

        //创建一个单线程的线程池
        pool = Executors.newSingleThreadExecutor();

        //获取一个ChannelProcessor
        final ChannelProcessor channelProcessor = getChannelProcessor();

        fileRunnable = new FileRunnable(filePath, positionFile, interval, charset, channelProcessor);

        //提交到线程池中
        pool.execute(fileRunnable);

        //调用父类的方法
        super.start();
    }

    @Override
    public synchronized void stop() {

        //停止
        fileRunnable.setFlag(false);

        //停止线程池
        pool.shutdown();

        while (!pool.isTerminated()) {
            logger.debug("Waiting for exec executor service to stop");

            try {
                //等500秒在停
                pool.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for exec executor service to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }
        super.stop();
    }


    private static class FileRunnable implements Runnable {

        private boolean flag = true;

        //偏移量
        private Long offset = 0L;

        private Long interval;

        private String charset;

        //可以直接从偏移量开始读取数据
        private RandomAccessFile randomAccessFile;

        //可以发送给channel的工具类
        private ChannelProcessor channelProcessor;

        private File posFile;

        public void setFlag(boolean flag) {
            this.flag = flag;
        }

        /**
         * 先于run方法执行，构造器只执行一次
         * 先看看有没有偏移量，如果有就接着读，如果没有就从头开始读
         */
        public FileRunnable(String filePath, String positionFile, Long interval, String charset, ChannelProcessor channelProcessor) {

            this.interval = interval;

            this.charset = charset;

            this.channelProcessor = channelProcessor;

            //读取偏移量,在positionFile文件
            posFile = new File(positionFile);

            if (!posFile.exists()) {
                //如果不存在就创建一个文件
                try {
                    posFile.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error("create positionFile file error: 创建保存偏移量的文件时失败", e);
                }
            }

            try {
                //读取文件的偏移量
                String offsetString = FileUtils.readFileToString(posFile);

                //以前读取过
                if (offsetString != null && !"".equals(offsetString)) {
                    //把偏移量穿换成long类型
                    offset = Long.parseLong(offsetString);
                }

            } catch (IOException e) {
                e.printStackTrace();
                logger.error("read positionFile file error: 读取保存偏移量的文件时失败", e);
            }

            try {
                //按照指定的偏移量读取数据
                randomAccessFile = new RandomAccessFile(filePath, "r");
                //按照指定的偏移量读取
                randomAccessFile.seek(offset);

            } catch (FileNotFoundException e) {
                e.printStackTrace();
                logger.error("read filePath file error: 读取文件时发生错误", e);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("randomAccessFile seek error", e);
            }
        }

        @Override
        public void run() {

            while (flag) {

                //读取文件中的新数据
                try {
                    String line = randomAccessFile.readLine();
                    if (line != null) {

                        //向channel发送数据
//                      channelProcessor.processEvent(EventBuilder.withBody(line, Charset.forName(charset)));//用下面的方式替代
                        //有数据进行处理，避免出现乱码
                        line = new String(line.getBytes("iso8859-1"), charset);
                        channelProcessor.processEvent(EventBuilder.withBody(line.getBytes()));

                        //获取偏移量,更新偏移量
                        offset = randomAccessFile.getFilePointer();

                        //将偏移量写入到位置文件中
                        FileUtils.writeStringToFile(posFile, offset.toString());

                    } else {
                        //没读到睡一会儿
                        Thread.sleep(interval);
                    }
                } catch (IOException e) {
                    logger.error("read randomAccessFile error", e);
                } catch (InterruptedException e) {
                    logger.error("sleep error", e);
                }
            }
        }
    }
}