package com.amazonaws.kinesisvideo.demoapp;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Payload {
    public Payload() {
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    @JsonProperty("action")
    private String action;
}
