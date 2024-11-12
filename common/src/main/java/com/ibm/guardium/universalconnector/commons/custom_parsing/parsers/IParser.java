package com.ibm.guardium.universalconnector.commons.custom_parsing.parsers;

public interface IParser {
    String parse(String key);

    void setPayload(String payload);

    boolean isInvalid();
}
