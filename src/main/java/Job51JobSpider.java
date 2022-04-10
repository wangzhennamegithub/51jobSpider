import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import scala.Tuple4;

import java.io.*;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;

public class Job51JobSpider {
    private static Logger logger = LogManager.getLogger(Job51JobSpider.class.getName());
    static Integer sleep=0;
    static Integer timeout=0;
    static String headless="";
    static String scheduleTime="";
    static String jobPageDir = "JobPage";
    static SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        logger.info("启动");
        String curDir = System.getProperty("user.dir");
        logger.info("curDir:"+curDir);
        Properties properties = new Properties();
        InputStream in = new FileInputStream(curDir+ File.separator+"config.properties");
        properties.load(in);
        headless = properties.getProperty("headless");
        logger.info("配置文件 headless:"+headless);
        sleep = Integer.valueOf(properties.getProperty("sleep"));
        timeout = Integer.valueOf(properties.getProperty("timeout"));
        logger.info("配置文件 timeout:"+timeout);
        scheduleTime = properties.getProperty("city_schedule_time");
        logger.info("配置文件 city_schedule_time:"+scheduleTime);




        WebDriverManager.chromedriver().setup();

        HashSet<String> crawlUrlSet=readJobUrl();
        logger.info("url个数:"+crawlUrlSet.size());
        BufferedWriter printer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("JobUrl.csv"),true), "UTF-8"));


        SparkConf conf = new SparkConf().setAppName("Job51JobSpider").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        ArrayList<String> crawlUrlList = new ArrayList<>(crawlUrlSet);
        JavaRDD<String> jobRdd = sc.parallelize(crawlUrlList,(int) Math.round(Math.sqrt(crawlUrlList.size())));
        JavaRDD<Tuple4<String, String,String,String>> jobHtmlRDD = jobRdd.map(new CrawlJob()).persist(StorageLevel.DISK_ONLY_2());
        List<Tuple4<String, String,String,String>> jobList = jobHtmlRDD.collect();
        for (Tuple4<String, String,String,String> job:jobList){
            //logger.info("url:"+url);
            new File(jobPageDir).mkdirs();
            writeFile(jobPageDir+File.separator + URLEncoder.encode( job._1(), "UTF-8" ),job._4());

            printer.write(job._1()+","+job._2()+","+job._3()+"\n");
            printer.flush();
        }
        printer.close();
    }

    private static HashSet<String> readJobUrl() throws Exception {
        HashSet<String> urlFinishSet = new HashSet<String>();
        String urlFileName="JobUrl.csv";
        FileInputStream urlFileInputStream = new FileInputStream(urlFileName);
        InputStreamReader urlInputStreamReader = new InputStreamReader(urlFileInputStream,"UTF-8");
        BufferedReader urlIn = new BufferedReader(urlInputStreamReader);
        String urlStr = null;
        while ((urlStr = urlIn.readLine()) != null) {
            String url = urlStr.split(",")[0];
            //logger.info("解析页数文件:"+pageFilename);
            urlFinishSet.add(url);
        }
        urlIn.close();
        logger.info("已完成的url数量:"+urlFinishSet.size());



        BufferedWriter printer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("JobUrl.csv"),true), "UTF-8"));

        HashSet<String> crawlUrlSet = new HashSet<String>();
        String pageFileName="JobFile.csv";
        FileInputStream fileInputStream = new FileInputStream(pageFileName);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream,"UTF-8");
        BufferedReader in = new BufferedReader(inputStreamReader);
        String str = null;
        while ((str = in.readLine()) != null) {
            String pageFilename = str.split(",")[0];
            pageFilename=pageFilename.replaceAll("\\\\", Matcher.quoteReplacement(File.separator));
            pageFilename=pageFilename.replaceAll("/",Matcher.quoteReplacement(File.separator));
            logger.info("解析页数文件:"+pageFilename);

            JSONArray jobList = JSONObject.parseObject(readFile(pageFilename)).getJSONArray("engine_jds");
            for (int i=0;i<jobList.size();i++){
                JSONObject jobJson = (JSONObject)jobList.get(i);
                String jobUrl = jobJson.getString("job_href");
                if(jobUrl.contains("jobs.51job.com")){
                    if(!urlFinishSet.contains(jobUrl)){
                        crawlUrlSet.add(jobUrl);
                        logger.info("添加url"+jobUrl);
                    }else
                    {
                        logger.info("url:"+jobUrl+"已抓取过，跳过");
                    }


                }else {
                    Date dNow = new Date( );
                    SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    logger.info("url:"+jobUrl+",为异常模板");
                    printer.write(jobUrl+",notsupport,"+ft.format(dNow)+"\n");
                    printer.flush();
                }
            }

        }
        printer.close();
        in.close();
        return crawlUrlSet;
    }

    public static void writeFile(String fileName,String text) throws IOException {
        FileWriter writer=new FileWriter(fileName);
        writer.write(text);
        writer.flush();
        writer.close();
    }

    public static String readFile(String fileName) throws Exception {
        FileInputStream fileInputStream = new FileInputStream(fileName);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream,"UTF-8");
        BufferedReader in = new BufferedReader(inputStreamReader);
        String linestr = null;
        String fileStr = "";
        while ((linestr = in.readLine()) != null) {

            fileStr+=linestr;
        }
        in.close();
        return fileStr;
    }

    private static class CrawlJob implements Function<String, Tuple4<String, String, String, String>> {
        @Override
        public Tuple4<String, String, String, String> call(String url) throws Exception {
            String curDir = System.getProperty("user.dir");
            String status = "";
            Tuple4<String, String, String, String> jobTup=null;
            BufferedWriter printer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("JobUrl.csv"),true), "UTF-8"));
            do{
                WebDriver driver = null;
                try {
                    ChromeOptions chromeOptions = new ChromeOptions();

                    chromeOptions.addArguments("blink-settings=imagesEnabled=false");
                    //chromeOptions.addArguments("--proxy-server=tunnel.alicloudecs.com:500");
                    chromeOptions.addArguments("--user-data-dir="+curDir+File.separator+"chrome");
                    if(headless.equals("on")){
                        chromeOptions.setHeadless(true);
                    }else{
                        chromeOptions.setHeadless(false);
                    }

                    driver = new ChromeDriver(chromeOptions);
                    logger.info("打开url:"+url);
                    driver.get(url);

                    Thread.sleep(sleep*1000);

                    String jobTitle="";
                    long startTime = System.currentTimeMillis();
                    WebElement stop = null;
                    do {
                        try {
                            Thread.sleep(500);
                            String urlTitle = driver.getTitle();
                            if(urlTitle.equals("滑动验证页面")){
                                logger.info("触发反爬虫验证,重新开始");
                                throw new Exception("CAPTCHA");
                            }

                            jobTitle = driver.findElement(By.className("cn")).findElement(By.tagName("h1")).getText();
                            //logger.info("jobTitle:"+jobTitle);
                        }catch (Exception e){
                            if(e.getMessage().equals("CAPTCHA")){
                                throw e;
                            }
                            //e.printStackTrace();
                        }

                        try {
                            stop=driver.findElement(By.className("research"));
                            if (stop!=null){
                                logger.info("很抱歉，你选择的职位目前已经暂停招聘");
                                Date dNow = new Date( );

                                printer.write(url+",stop,"+ft.format(dNow)+"\n");
                                printer.flush();
                                throw new Exception("stop");
                            }
                        }catch (Exception e){
                            if(e.getMessage().equals("stop")){
                                throw e;
                            }
                        }
                        long endTime = System.currentTimeMillis();
                        if((endTime-startTime)/1000>timeout){
                            logger.info("访问超时,重新开始");
                            throw new Exception("timeout");
                        }
                    }while (jobTitle.equals(""));

                    String html = driver.getPageSource();
                    new File(jobPageDir).mkdirs();
                    writeFile(jobPageDir+File.separator + URLEncoder.encode( url, "UTF-8" ),html);

                    Date dNow = new Date( );
                    printer.write(url+",ok,"+ft.format(dNow)+"\n");
                    printer.flush();

                    jobTup = new Tuple4<String, String, String, String>(url, "OK", ft.format(dNow), html);
                    status="OK";
                }catch (Exception e){
                    if(e.getMessage().equals("CAPTCHA")
                            ||e.getMessage().equals("none proxy")
                            ||e.getMessage().equals("timeout")
                            ||e.getMessage().contains("unknown error: net::ERR_TUNNEL_CONNECTION_FAILED")
                            ||e.getMessage().contains("unknown error: net::ERR_PROXY_CONNECTION_FAILED")){
//                        Document doc = Jsoup.connect("http://d.jghttp.alicloudecs.com/getip?num=1&type=1&pro=&city=0&yys=0&port=11&time=4&ts=0&ys=0&cs=0&lb=1&sb=0&pb=45&mr=1&regions=").get();
//                        proxy = doc.body().html();
//                        logger.info("设置代理:"+proxy);
                    }
                    else if(e.getMessage().equals("stop")){
                        status="stop";
                    }
                    else {
                        e.printStackTrace();
                        //logger.info(e.getMessage());
                    }
                    //e.printStackTrace();
                }
                finally {
                    if (driver!=null){
                        driver.quit();
                    }

                }
            }while (!status.equals("OK")&&!status.equals("stop"));

            printer.close();
            Thread.sleep(30*1000);
            return jobTup;
        }
    }
}
