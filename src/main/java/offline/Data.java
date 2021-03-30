package offline;

import java.sql.Timestamp;

public class Data {
    String name;
    Long loginTime;
    Integer num;

    public Data() {
    }


    public Data(String name, Long loginTime, Integer num) {
        this.name = name;
        this.loginTime = loginTime;
        this.num = num;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(Long loginTime) {
        this.loginTime = loginTime;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }
}


