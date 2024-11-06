package com.ibm.guardium.universalconnector.commons.custom_parsing.parsers;

import com.ibm.guardium.universalconnector.commons.custom_parsing.parsers.json_parser.JsonUtil;

import java.util.Map;

public class JsonParser implements IParser {

    private JsonUtil util = new JsonUtil();
    Map<String, String> extractedProperties;

    @Override
    public String parse(String key) {
        if (key == null)
            return null;
        return extractedProperties.get(key);
    }

    @Override
    public void setPayload(String payload) {
        extractedProperties = util.getMap(payload);
    }

    @Override
    public boolean isValid() {
        return extractedProperties == null || extractedProperties.isEmpty();
    }
}
