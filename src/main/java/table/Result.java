package table;

import java.sql.Timestamp;

public class Result {
    String name;
    Long cnt;
    Timestamp start;
    Timestamp end;
    Timestamp rowTime;

    public Result() {

    }

    public Result(String name,
                  Long cnt,
                  Timestamp start,
                  Timestamp end,
                  Timestamp rowTime) {
        this.name = name;
        this.cnt = cnt;
        this.end = end;
        this.rowTime = rowTime;
        this.start = start;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getCnt() {
        return cnt;
    }

    public void setCnt(Long cnt) {
        this.cnt = cnt;
    }

    public Timestamp getStart() {
        return start;
    }

    public void setStart(Timestamp start) {
        this.start = start;
    }

    public Timestamp getEnd() {
        return end;
    }

    public void setEnd(Timestamp end) {
        this.end = end;
    }

    public Timestamp getRowTime() {
        return rowTime;
    }

    public void setRowTime(Timestamp rowTime) {
        this.rowTime = rowTime;
    }

    @Override
    public String toString() {
        return "Result{" +
                "name='" + name + '\'' +
                ", cnt=" + cnt +
                ", start=" + start +
                ", end=" + end +
                ", rowTime=" + rowTime +
                '}';
    }
}
