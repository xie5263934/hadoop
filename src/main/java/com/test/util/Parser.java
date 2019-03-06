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
        missing = false;
        malformed = false;
        valid = false;
        if (this.value.charAt(87) == '+') {
            airTemperature = Integer.parseInt(this.value.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(this.value.substring(87, 92));
        }
        quality = this.value.substring(92, 93);
        if (airTemperature == MISSING) {
            missing = true;
        } else if (airTemperature != MISSING && quality.matches("[01459]")) {
            valid = true;
        } else {
            malformed = true;
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
        return valid;
    }

    public boolean isMissing() {
        return missing;
    }

    public boolean isMalformed() {
        return malformed;
    }

    public int getTemperatue() {
        return airTemperature;
    }
}
