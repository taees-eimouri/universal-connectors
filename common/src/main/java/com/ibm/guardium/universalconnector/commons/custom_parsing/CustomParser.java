package com.ibm.guardium.universalconnector.commons.custom_parsing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.guardium.universalconnector.commons.custom_parsing.excepton.InvalidConfigurationException;
import com.ibm.guardium.universalconnector.commons.custom_parsing.parsers.IParser;
import com.ibm.guardium.universalconnector.commons.structures.*;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static com.ibm.guardium.universalconnector.commons.custom_parsing.PropertyConstant.*;
import static com.ibm.guardium.universalconnector.commons.structures.Accessor.DATA_TYPE_GUARDIUM_SHOULD_NOT_PARSE_SQL;
import static com.ibm.guardium.universalconnector.commons.structures.Accessor.DATA_TYPE_GUARDIUM_SHOULD_PARSE_SQL;
import static com.ibm.guardium.universalconnector.commons.structures.SessionLocator.PORT_DEFAULT;

public abstract class CustomParser {
    private static final Logger logger = LogManager.getLogger(CustomParser.class);
    private static final InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();
    protected Map<String, String> properties;
    private final ObjectMapper mapper;
    IParser parser;
    protected boolean parseUsingSniffer = false;
    protected boolean hasSqlParsing = false;
    protected boolean parseUsingCustomParser = false;

    public CustomParser(ParserFactory.ParserType parserType) throws InvalidConfigurationException {
        parser = new ParserFactory().getParser(parserType);
        mapper = new ObjectMapper();

        // We only need to read the properties file once and then we validate it.
        properties = getProperties();
        if (!isValid())
            throw new InvalidConfigurationException("The configuration file is invalid.");
    }

    public Record parseRecord(String payload) {
        if (payload == null) {
            logger.error("The provided payload is null.");
            return null;
        }

        parser.setPayload(payload);
        if (parser.isInvalid())
            return null;
        return extractRecord();
    }

    private Record extractRecord() {
        Record record = new Record();

        record.setSessionId(getSessionId());
        record.setDbName(getDbName());
        record.setAppUserName(getAppUserName());
        String sqlString = getSqlString();
        record.setException(getException(sqlString));
        record.setAccessor(getAccessor());
        record.setSessionLocator(getSessionLocator(record.getSessionId()));
        record.setTime(getTimestamp());

        if (record.isException())
            record.setData(getData(sqlString));

        return record;
    }

    protected String getValue(String fieldName) {
        String value = properties.get(fieldName);
        if (value == null)
            return null;

        // If it is static literal we dont need custom parser
        if (value.startsWith("{") && value.endsWith("}"))
            return value.substring(1, value.indexOf("}"));

        return parse(value);
    }

    protected String parse(String key) {
        return parser.parse(key);
    }

    // method to handle exception type and description
    protected ExceptionRecord getException(String sqlString) {
        String exceptionTypeId = getExceptionTypeId(); // Get the error message
        if (exceptionTypeId.isEmpty())
            return null;

        ExceptionRecord exceptionRecord = new ExceptionRecord();
        exceptionRecord.setExceptionTypeId(exceptionTypeId);
        exceptionRecord.setDescription(getExceptionDescription());
        exceptionRecord.setSqlString(sqlString);

        return exceptionRecord;
    }

    protected String getExceptionDescription() {
        return DEFAULT_STRING;
    }

    protected String getExceptionTypeId() {
        String value = getValue(EXCEPTION_TYPE_ID);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getAppUserName() {
        String value = getValue(APP_USER_NAME);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getClientIpv6() {
        String value = getValue(CLIENT_IPV6);
        return value != null ? value : DEFAULT_IPV6;
    }

    protected String getClientIp() {
        String value = getValue(CLIENT_IP);
        return value != null ? value : DEFAULT_IP;
    }

    protected Data getDataForException(String sqlString) {
        Data data = new Data();
        data.setOriginalSqlCommand(sqlString);
        return data;
    }

    protected Data getData(String sqlString) {
        if (!hasSqlParsing || parseUsingSniffer) {
            return null;
        }

        Data data = new Data();
        // If it reaches out this point it is a regex parsing and object and verb are
        // not null
        String object = getValue(OBJECT);
        String verb = getValue(VERB);
        Construct construct = new Construct();
        Sentence sentence = new Sentence(verb);
        SentenceObject sentenceObject = new SentenceObject(object);
        sentence.getObjects().add(sentenceObject);
        construct.sentences.add(sentence);
        construct.setFullSql(sqlString);

        data.setConstruct(construct);
        data.setOriginalSqlCommand(getOriginalSqlCommand());
        return data;
    }

    protected Boolean isIpv6() {
        String value = getValue(IS_IPV6);
        return Boolean.parseBoolean(value);
    }

    protected Integer getMinDst() {
        Integer value = convertToInt(MIN_DST, getValue(MIN_DST));
        return value != null ? value : 0;
    }

    protected Integer getMinOffsetFromGMT() {
        Integer value = convertToInt(MIN_OFFSET_FROM_GMT, getValue(MIN_OFFSET_FROM_GMT));
        return value != null ? value : ZERO;
    }

    protected String getOriginalSqlCommand() {
        String value = getValue(ORIGINAL_SQL_COMMAND);
        return value != null ? value : getSqlString();
    }

    protected String getServerIp() {
        String value = getValue(SERVER_IP);
        return value != null ? value : DEFAULT_IP;
    }

    protected String getServerIpv6() {
        String value = getValue(SERVER_IPV6);
        return value != null ? value : DEFAULT_IPV6;
    }

    // method to handle the SQL command that caused the exception
    protected String getSqlString() {
        String value = getValue(SQL_STRING);
        return value != null ? value : DEFAULT_STRING; // Set the SQL command that caused the exception
    }

    // In this setTimestamp method now parses the timestamp from the payload and
    // sets the timestamp, minOffsetFromGMT, and minDst fields in the Time object of
    // the Record. If the timestamp is not available, it sets default values.
    protected Time getTimestamp() {
        String value = getValue(TIMESTAMP);
        Time time;
        if (value != null) {
            time = parseTimestamp(value);
        } else {
            time = new Time(0L, 0, 0);
        }
        return time;
    }

    protected SessionLocator getSessionLocator(String sessionId) {
        SessionLocator sessionLocator = new SessionLocator();

        // set default values
        sessionLocator.setIpv6(false);
        sessionLocator.setClientIpv6(DEFAULT_IPV6);
        sessionLocator.setServerIpv6(DEFAULT_IPV6);
        sessionLocator.setClientIp(DEFAULT_IP);
        sessionLocator.setServerIp(DEFAULT_IP);

        boolean isIpV6 = isIpv6();
        String clientIp = getClientIp();
        String clientIpv6 = getClientIpv6();
        if (isIpV6 && inetAddressValidator.isValidInet6Address(clientIpv6)) {
            // If client IP is IPv6, set both client and server to IPv6
            sessionLocator.setIpv6(true);
            sessionLocator.setClientIpv6(clientIpv6);
            sessionLocator.setServerIpv6(getServerIpv6()); // Set server IP to default IPv6

        } else if (inetAddressValidator.isValidInet4Address(clientIp)) {
            // If client IP is IPv4, set both client and server IP to IPv4
            sessionLocator.setClientIp(clientIp);
            // Cloud Databases: Set server IP to 0.0.0.0
            sessionLocator.setServerIp(getServerIp());
        }

        // Set port numbers
        sessionLocator.setClientPort(getClientPort(sessionId));
        sessionLocator.setServerPort(getServerPort(sessionId));

        return sessionLocator;
    }

    public static Time parseTimestamp(String timestamp) {
        ZonedDateTime date = ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_DATE_TIME);
        long millis = date.toInstant().toEpochMilli();
        int minOffset = date.getOffset().getTotalSeconds() / 60;
        int minDst = date.getZone().getRules().isDaylightSavings(date.toInstant()) ? 60 : 0;
        return new Time(millis, minOffset, minDst);
    }

    // Updated method to check accessor.dataType and populate original_sql_command
    // or construct
    protected Accessor getAccessor() {
        Accessor accessor = new Accessor();

        accessor.setServiceName(getServiceName());
        accessor.setDbUser(getDbUser());
        accessor.setDbProtocolVersion(getDbProtocolVersion());
        accessor.setDbProtocol(getDbProtocol());
        accessor.setServerType(getServerType());
        accessor.setServerOs(getServerOs());
        accessor.setServerDescription(getServerDescription());
        accessor.setServerHostName(getServerHostName());
        accessor.setClientHostName(getClientHostName());
        accessor.setClient_mac(getClientMac());
        accessor.setClientOs(getClientOs());
        accessor.setCommProtocol(getCommProtocol());
        accessor.setOsUser(getOsUser());
        accessor.setSourceProgram(getSourceProgram());
        accessor.setLanguage(getLanguage());
        accessor.setDataType(getDataType());

        return accessor;
    }

    protected String getServiceName() {
        String value = getValue(SERVICE_NAME);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getDbUser() {
        String value = getValue(DB_USER);
        return value != null ? value : DATABASE_NOT_AVAILABLE;
    }

    protected String getDbName() {
        String value = getValue(DB_NAME);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getDbProtocol() {
        String value = getValue(DB_PROTOCOL);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getServerOs() {
        String value = getValue(SERVER_OS);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getClientOs() {
        String value = getValue(CLIENT_OS);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getClientHostName() {
        String value = getValue(CLIENT_HOSTNAME);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getCommProtocol() {
        String value = getValue(COMM_PROTOCOL);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getDbProtocolVersion() {
        String value = getValue(DB_PROTOCOL_VERSION);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getOsUser() {
        String value = getValue(OS_USER);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getSourceProgram() {
        String value = getValue(SOURCE_PROGRAM);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getClientMac() {
        String value = getValue(CLIENT_MAC);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getServerDescription() {
        String value = getValue(SERVER_DESCRIPTION);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getServerHostName() {
        String value = getValue(SERVER_HOSTNAME);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getServerType() {
        // this has been validated before
        if (parseUsingSniffer)
            return SqlParser.getServerType(properties.get(SNIFFER_PARSER));

        String value = getValue(SERVER_TYPE);
        return value != null ? value : DEFAULT_STRING;
    }

    protected String getLanguage() {
        // this has been validated before
        if (parseUsingSniffer)
            return properties.get(SNIFFER_PARSER);

        return Accessor.LANGUAGE_FREE_TEXT_STRING;
    }

    protected String getDataType() {
        if (parseUsingSniffer)
            return properties.get(DATA_TYPE_GUARDIUM_SHOULD_PARSE_SQL);

        return DATA_TYPE_GUARDIUM_SHOULD_NOT_PARSE_SQL;
    }

    protected String getSessionId() {
        String value = getValue(SESSION_ID);
        return value != null ? value : DEFAULT_STRING;
    }

    protected Integer getClientPort(String sessionId) {
        if (sessionId.isEmpty())
            return PORT_DEFAULT;

        Integer value = convertToInt(CLIENT_PORT, getValue(CLIENT_PORT));
        return value != null ? value : PORT_DEFAULT;
    }

    protected Integer getServerPort(String sessionId) {
        if (sessionId.isEmpty())
            return PORT_DEFAULT;

        Integer value = convertToInt(SERVER_PORT, getValue(SERVER_PORT));
        return value != null ? value : PORT_DEFAULT;
    }

    public abstract String getConfigFilePath();

    public Map<String, String> getProperties() {
        try {
            String content = new String(Files.readAllBytes(Paths.get(getConfigFilePath())));
            return mapper.readValue(content, HashMap.class);
        } catch (Exception e) {
            logger.error("Error reading properties from config file", e);
            return null;
        }
    }

    protected Integer convertToInt(String fieldName, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            if (logger.isDebugEnabled())
                logger.debug("{} {}is not a valid integer.", fieldName, value);
        }
        return null;
    }

    protected boolean isValid() {
        if (properties == null) {
            logger.error("The provided config file is invalid.");
            return false;
        }

        hasSqlParsing = SqlParser.hasSqlParsing(properties);
        parseUsingSniffer = hasSqlParsing && SqlParser.isSnifferParsing(properties);
        parseUsingCustomParser = hasSqlParsing && SqlParser.isCustomParsing(properties);

        SqlParser.ValidityCase isValid = SqlParser.isValid(properties, hasSqlParsing, parseUsingSniffer,
                parseUsingCustomParser);
        if (!isValid.equals(SqlParser.ValidityCase.VALID)) {
            logger.error(isValid.getDescription());
            return false;
        }

        return true;
    }
}