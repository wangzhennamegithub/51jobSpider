import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.*;
import java.net.URLEncoder;
import java.util.regex.Matcher;

public class Job51MapSpider {
    private static Logger logger = LogManager.getLogger(Job51MapSpider.class.getName());

    public static void main(String[] args) throws Exception {
        logger.info("启动");
        String curDir = System.getProperty("user.dir");
        logger.info("curDir:"+curDir);

        String jobMapDir = "JobMap";
        new File(jobMapDir).mkdirs();

        BufferedWriter printer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("JobMap.csv"),true), "UTF-8"));

        String mapUrl = "https://search.51job.com/jobsearch/bmap/map.php?jobid=";

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
                String jobId = jobJson.getString("jobid");
                logger.info("抓取jobid:"+jobId);
                logger.info("第"+(i+1)+"/"+jobList.size()+"个文件");

                Document doc = Jsoup.connect(mapUrl+jobId).cookie("1","1").get();
                String html = doc.body().html();

                writeFile(jobMapDir+File.separator + jobId+".html",html);

                printer.write(jobId+","+jobMapDir+File.separator + jobId+".html"+"\n");
                printer.flush();
            }

        }
        in.close();




        printer.close();
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

    public static void writeFile(String fileName,String text) throws IOException {
        FileWriter writer=new FileWriter(fileName);
        writer.write(text);
        writer.flush();
        writer.close();
    }
}
