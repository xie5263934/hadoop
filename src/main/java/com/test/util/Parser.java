package com.test.util;

public class Parser {
    private static final int MISSING = 9999;
    private String value;
    private boolean missing;
    private boolean malformed;
    private boolean valid;
    private int airTemperature;
    private String quality;

    public void parse(String value) {
        this.value = value;
        this.missing = false;
        this.malformed = false;
        this.valid = false;
        if (this.value.charAt(87) == '+') {
            this.airTemperature = Integer.parseInt(this.value.substring(88, 92));
        } else {
            this.airTemperature = Integer.parseInt(this.value.substring(87, 92));
        }
        this.quality = this.value.substring(92, 93);
        if (this.airTemperature == this.MISSING) {
            this.missing = true;
        } else if (this.airTemperature != MISSING && this.quality.matches("[01459]")) {
            this.valid = true;
        } else {
            this.malformed = true;
        }
    }

    public String getQuality() {
        return this.quality;
    }

    public String getStationId() {
        String pre = value.substring(4, 10);
        String sub = value.substring(10, 15);
        return pre + "-" + sub;
    }


    public String getYear() {
        return this.value.substring(15, 19);
    }


    public boolean isValid() {
        return this.valid;
    }

    public boolean isMissing() {
        return this.missing;
    }

    public boolean isMalformed() {
        return this.malformed;
    }

    public int getTemperature() {
        return this.airTemperature;
    }
}
