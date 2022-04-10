import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Job51CitySpider {
    public static void main(String[] args) throws Exception {
        Document cityDoc = Jsoup.connect("https://search.51job.com/list/030200,030203,0000,00,0,99,+,2,1.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=1&dibiaoid=0&line=&welfare=")
                .header("Accept", "application/json, text/javascript, */*; q=0.01")
                .proxy("tunnel.alicloudecs.com",500)
                .get();

        //System.out.println(cityDoc.body().text());
        JSONObject json = JSONObject.parseObject(cityDoc.body().text());
        int totalPage = Integer.valueOf(json.getString("total_page"));
        System.out.println("totalPage:"+totalPage);

        String cityPageDir = "CityPage";
        new File(cityPageDir+File.separator+"广州"+File.separator+"海珠区").mkdirs();
        BufferedWriter printer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("JobFile.csv"),true), "UTF-8"));
        for (int i=1;i<=totalPage;i++){
            System.out.println("抓取"+i+"/"+totalPage+"页");
            String pageJson="";
            do {
                try {
                    Document pageDoc= Jsoup.connect("https://search.51job.com/list/030200,030203,0000,00,0,99,+,2,"+i+".html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=1&dibiaoid=0&line=&welfare=")
                            .header("Accept", "application/json, text/javascript, */*; q=0.01")
                            .proxy("tunnel.alicloudecs.com",500)
                            .get();
                    pageJson=pageDoc.body().text();
                }catch (Exception e){
                    //e.printStackTrace();
                }
            }while (pageJson.equals(""));

            Date dNow = new Date( );
            SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            writeFile(cityPageDir+File.separator+"广州"+File.separator+"海珠区" + File.separator + +i+".json", pageJson);
            printer.write(cityPageDir+File.separator+"广州" +File.separator+"海珠区"+ File.separator + +i+".json,"+ft.format(dNow)+"\n");
            printer.flush();
//            JSONObject pageJsonObj = JSONObject.parseObject(pageJson);
//            JSONArray jobList = pageJsonObj.getJSONArray("engine_jds");
//            for (int j=0;j<jobList.size();j++){
//                JSONObject jobJson= (JSONObject) jobList.get(j);
//                String jobName = jobJson.getString("job_name");
//                System.out.println(""+jobName);
//            }

        }
        printer.close();


    }

    public static void writeFile(String fileName,String text) throws IOException {
        FileWriter writer=new FileWriter(fileName);
        writer.write(text);
        writer.flush();
        writer.close();
    }
}
