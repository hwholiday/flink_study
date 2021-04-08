package message;

public class Message {
    String BizTag;
    Integer Uid;
    Long CreateTime;
    String Event;
    String Tag;

    public Message() {
    }

    public Message(String BizTag,
                   Integer Uid,
                   Long CreateTime,
                   String Event,
                   String Tag) {
        this.BizTag = BizTag;
        this.Uid = Uid;
        this.CreateTime = CreateTime;
        this.Event = Event;
        this.Tag = Tag;
    }

    public String getBizTag() {
        return BizTag;
    }

    public void setBizTag(String bizTag) {
        BizTag = bizTag;
    }

    public Integer getUid() {
        return Uid;
    }

    public void setUid(Integer uid) {
        Uid = uid;
    }

    public Long getCreateTime() {
        return CreateTime;
    }

    public void setCreateTime(Long createTime) {
        CreateTime = createTime;
    }

    public String getEvent() {
        return Event;
    }

    public void setEvent(String event) {
        Event = event;
    }

    public String getTag() {
        return Tag;
    }

    public void setTag(String tag) {
        Tag = tag;
    }
}
