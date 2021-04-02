package msg;

public class Msg {
    public static final int APPID = 1;
    public static final int BROADCAST = 2;
    public static final int BROADCAST_GROUP = 3;
    public static final int ROOM = 4;
    public static final int SINGLE_CHAT = 5;

    public int userId;      // 发送者
    public int msgType;     // 消息类型
    public int to;          //
    public Long sendTime;
    public Long msgCount;

    public Msg(int userId, int msgType, int to, Long sendTime, Long msgCount) {
        this.msgType = msgType;
        this.userId = userId;
        this.to = to;
        this.sendTime = sendTime;
        this.msgCount = msgCount;
    }

    public Msg() {

    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getMsgType() {
        return msgType;
    }

    public void setMsgType(int msgType) {
        this.msgType = msgType;
    }

    public int getTo() {
        return to;
    }

    public Long getMsgCount() {
        return msgCount;
    }

    public void setMsgCount(Long msgCount) {
        this.msgCount = msgCount;
    }

    public Long getSendTime() {
        return sendTime;
    }

    public void setSendTime(Long sendTime) {
        this.sendTime = sendTime;
    }

    public void setTo(int to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "Msg{" +
                "userId=" + userId +
                ", msgType=" + msgType +
                ", to=" + to +
                ", sendTime=" + sendTime +
                ", msgCount=" + msgCount +
                '}';
    }
}
