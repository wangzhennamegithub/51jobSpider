public class JobInfo {
    private String companyName;
    private String companyHref;
    private String companyType;
    private String companySize;
    private String jobUrl;
    private String jobName;
    private String updateDate;
    private String salary;
    private String address;
    private String jobId;
    private String experience;
    private String education;
    private String crawlDate;
    private String city;
    private String companyind;
    private String longitude;
    private String latitude;

    public void setLongitude(String longitude){
        this.longitude=longitude;
    }

    String getLongitude() {
        return longitude;
    }

    public void setLatitude(String latitude){
        this.latitude=latitude;
    }

    String getLatitude() {
        return latitude;
    }

    public void setCompanyind(String companyind){
        this.companyind=companyind;
    }

    String getCompanyind() {
        return companyind;
    }

    public void setCompanyHref(String companyHref){
        this.companyHref=companyHref;
    }

    String getCompanyHref() {
        return companyHref;
    }

    public void setCompanyName(String companyName) {
        this.companyName=companyName;
    }

    String getCompanyName() {
        return companyName;
    }

    public void setCompanyType(String companyType) {
        this.companyType=companyType;
    }

    String getCompanyType() {
        return companyType;
    }

    public void setCompanySize(String companySize) {
        this.companySize=companySize;
    }

    String getCompanySize() {
        return companySize;
    }

    public void setJobUrl(String jobUrl) {
        this.jobUrl=jobUrl;
    }

    String getJobUrl() {
        return jobUrl;
    }

    public void setJobName(String jobName) {
        this.jobName=jobName;
    }

    String getJobName() {
        return jobName;
    }

    public void setUpdateDate(String updateDate) {
        this.updateDate=updateDate;
    }

    String getIpdateDate() {
        return updateDate;
    }

    public void setSalary(String salary) {
        this.salary=salary;
    }

    String getSalary() {
        return salary;
    }

    public void setAddress(String address) {
        this.address=address;
    }

    String getAddress() {
        return address;
    }

    public void setJobId(String jobId) {
        this.jobId=jobId;
    }

    String getJobId() {
        return jobId;
    }

    public void setExperience(String experience) {
        this.experience=experience;
    }

    String getExperience() {
        return experience;
    }

    public void setEducation(String education) {
        this.education=education;
    }

    String getEducation() {
        return education;
    }

    public void setCrawlDate(String crawlDate) {
        this.crawlDate=crawlDate;
    }

    String getCrawlDate() {
        return crawlDate;
    }

    public void setCity(String city) {
        this.city=city;
    }

    String getCity() {
        return city;
    }

    @Override
    public String toString() {
        String positionInfo = "\""+companyName + "\","+
                "\""+companyHref+"\","+
                "\""+companyType+"\","+
                "\""+companySize +"\","+
                "\""+jobUrl+"\","+
                "\""+jobName+"\","+
                "\""+updateDate+"\","+
                "\""+salary+"\","+
                "\""+jobId +"\","+
                "\""+experience+"\","+
                "\""+education+"\","+
                "\""+crawlDate+"\","+
                "\""+city+"\","+
                "\""+companyind+"\","+
                "\""+longitude+"\","+
                "\""+latitude+"\","+
                "\""+address+"\"";
        return positionInfo;
    }
}
