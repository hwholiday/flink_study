package offline;

import java.sql.Timestamp;

public class Data {
    String name;
    Timestamp loginTime;
    Integer num;

    public Data() {
    }


    public Data(String name, Timestamp loginTime, Integer num) {
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

    public Timestamp getLoginTime() {
        return loginTime;
    }

    public void setLoginTime(Timestamp loginTime) {
        this.loginTime = loginTime;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }
}


