import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json4s.jackson.Json;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;

public class Job51JobExtract {
    private static Logger logger = LogManager.getLogger(Job51JobExtract.class.getName());
    public static void main(String[] args) throws Exception {
        String curDir = System.getProperty("user.dir");
        logger.info("curDir:"+curDir);

        logger.info("处理地图文件");
        HashMap<String, List> jobGeographyMap = new HashMap<String, List>();
        String mapFileName="JobMap.csv";
        FileInputStream mapInputStream = new FileInputStream(mapFileName);
        InputStreamReader mapInputStreamReader = new InputStreamReader(mapInputStream,"UTF-8");
        BufferedReader mapIn = new BufferedReader(mapInputStreamReader);
        String map = null;
        while ((map = mapIn.readLine()) != null) {
            String jobId = map.split(",")[0];
            String mapFile = map.split(",")[1];
            mapFile=mapFile.replaceAll("\\\\", Matcher.quoteReplacement(File.separator));
            mapFile=mapFile.replaceAll("/",Matcher.quoteReplacement(File.separator));
            Document doc = Jsoup.parse(new File(mapFile), "UTF-8");
            Element geography = doc.selectFirst("input#end");
            String lng = geography.attr("lng");
            String lat = geography.attr("lat");
            String address = geography.attr("value");
            ArrayList<String> geographyList = new ArrayList<String>();
            geographyList.add(lng);
            geographyList.add(lat);
            geographyList.add(address);
            logger.info("map:"+jobId+","+mapFile+","+lng+","+lat+","+address);
            jobGeographyMap.put(jobId,geographyList);
        }
        mapIn.close();


        BufferedWriter printer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("job.csv"),true), "UTF-8"));

        HashMap<String, JobInfo> jobMap = new HashMap<String,JobInfo>();
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
                String jobName = jobJson.getString("job_name");
                String companyName = jobJson.getString("company_name");
                String companyHref = jobJson.getString("company_href");
                String companyType = jobJson.getString("companytype_text");
                String companySize = jobJson.getString("companysize_text");
                String updateDate = jobJson.getString("updatedate");
                String salary = jobJson.getString("providesalary_text");
                String jobId = jobJson.getString("jobid");
                JSONArray attribute_text = jobJson.getJSONArray("attribute_text");
                String city = jobJson.getString("workarea_text");
                String companyind = jobJson.getString("companyind_text");

                ArrayList<String> geographyList = (ArrayList<String>) jobGeographyMap.get(jobId);
                String lng = geographyList.get(0);
                String lat = geographyList.get(1);
                String address = geographyList.get(2);


                String experience="";
                if (attribute_text.size()>=2){
                    experience = attribute_text.getString(1);
                }

                String education="";
                if (attribute_text.size()>=3){
                    education = attribute_text.getString(2);
                }


                JobInfo job = new JobInfo();
                job.setJobUrl(jobUrl);
                job.setJobName(jobName);
                job.setCompanyName(companyName);
                job.setCompanyHref(companyHref);
                job.setCompanyType(companyType);
                job.setCompanySize(companySize);
                job.setUpdateDate(updateDate);
                job.setSalary(salary);
                job.setExperience(experience);
                job.setEducation(education);
                job.setJobId(jobId);
                job.setCrawlDate(str.split(",")[1]);
                job.setCity(city);
                job.setCompanyind(companyind);
                job.setLongitude(lng);
                job.setLatitude(lat);
                job.setAddress(address);

                jobMap.put(jobUrl,job);
                logger.info("job:"+job);
                printer.write(job+"\n");
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
}
