public class KafkaTestBean {

    private Double bandWidth;

    private Long app_time;

    private Double packet;

    private String networkLineId;


    public KafkaTestBean(Double bandWidth, Long app_time, Double packet, String networkLineId) {
        this.bandWidth = bandWidth;
        this.app_time = app_time;
        this.packet = packet;
        this.networkLineId = networkLineId;
    }

    public Double getPacket() {
        return packet;
    }

    public void setPacket(Double packet) {
        this.packet = packet;
    }

    public KafkaTestBean() {
    }

    public Double getBandWidth() {
        return bandWidth;
    }

    public void setBandWidth(Double bandWidth) {
        this.bandWidth = bandWidth;
    }

    public Long getApp_time() {
        return app_time;
    }

    public void setApp_time(Long app_time) {
        this.app_time = app_time;
    }

    public String getNetworkLineId() {
        return networkLineId;
    }

    public void setNetworkLineId(String networkLineId) {
        this.networkLineId = networkLineId;
    }
}
