package pl.allegro.tech.hermes.api;

import javax.ws.rs.core.Response;

import static javax.ws.rs.core.Response.Status.*;

public enum ErrorCode {
    TIMEOUT(REQUEST_TIMEOUT),
    TOPIC_ALREADY_EXISTS(BAD_REQUEST),
    TOPIC_NOT_EXISTS(NOT_FOUND),
    GROUP_NOT_EXISTS(NOT_FOUND),
    GROUP_NAME_IS_INVALID(BAD_REQUEST),
    SUBSCRIPTION_NOT_EXISTS(BAD_REQUEST),
    SUBSCRIPTION_ALREADY_EXISTS(BAD_REQUEST),
    VALIDATION_ERROR(BAD_REQUEST),
    INTERNAL_ERROR(INTERNAL_SERVER_ERROR),
    FORMAT_ERROR(BAD_REQUEST),
    GROUP_NOT_EMPTY(FORBIDDEN),
    TOPIC_NOT_EMPTY(FORBIDDEN),
    GROUP_ALREADY_EXISTS(BAD_REQUEST),
    OPERATION_DISABLED(NOT_ACCEPTABLE),
    OTHER(INTERNAL_SERVER_ERROR),
    UNAVAILABLE_RATE(BAD_REQUEST),
    SINGLE_MESSAGE_READER_EXCEPTION(INTERNAL_SERVER_ERROR),
    PARTITIONS_NOT_FOUND_FOR_TOPIC(NOT_FOUND),
    OFFSET_NOT_FOUND_EXCEPTION(NOT_FOUND),
    OFFSETS_NOT_AVAILABLE_EXCEPTION(INTERNAL_SERVER_ERROR),
    UNABLE_TO_MOVE_OFFSETS_EXCEPTION(INTERNAL_SERVER_ERROR),
    BROKERS_CLUSTER_NOT_FOUND_EXCEPTION(NOT_FOUND),
    BROKERS_CLUSTER_COMMUNICATION_EXCEPTION(INTERNAL_SERVER_ERROR),
    SIMPLE_CONSUMER_POOL_EXCEPTION(INTERNAL_SERVER_ERROR),
    RETRANSMISSION_EXCEPTION(INTERNAL_SERVER_ERROR),
    TOKEN_NOT_PROVIDED(FORBIDDEN),
    GROUP_NOT_PROVIDED(FORBIDDEN),
    AUTH_ERROR(FORBIDDEN),
    SCHEMA_REPOSITORY_INTERNAL_ERROR(INTERNAL_SERVER_ERROR),
    SCHEMA_BAD_REQUEST(BAD_REQUEST),
    SCHEMA_COULD_NOT_BE_LOADED(INTERNAL_SERVER_ERROR),
    SCHEMA_VERSION_DOES_NOT_EXIST(BAD_REQUEST),
    SCHEMA_ALREADY_EXISTS(BAD_REQUEST),
    AVRO_SCHEMA_INVALID_METADATA(BAD_REQUEST),
    SUBSCRIPTION_ENDPOINT_ADDRESS_CHANGE_EXCEPTION(INTERNAL_SERVER_ERROR),
    OAUTH_PROVIDER_NOT_EXISTS(NOT_FOUND),
    OAUTH_PROVIDER_ALREADY_EXISTS(BAD_REQUEST),
    CROWD_GROUPS_COULD_NOT_BE_LOADED(INTERNAL_SERVER_ERROR),
    TOPIC_BLACKLISTED(FORBIDDEN),
    THROUGHPUT_QUOTA_VIOLATION(429),
    TOPIC_NOT_UNBLACKLISTED(BAD_REQUEST),
    TOPIC_CONSTRAINTS_ALREADY_EXIST(BAD_REQUEST),
    TOPIC_CONSTRAINTS_DO_NOT_EXIST(BAD_REQUEST),
    SUBSCRIPTION_CONSTRAINTS_ALREADY_EXIST(BAD_REQUEST),
    SUBSCRIPTION_CONSTRAINTS_DO_NOT_EXIST(BAD_REQUEST),
    OWNER_SOURCE_NOT_FOUND(NOT_FOUND),
    OWNER_SOURCE_DOESNT_SUPPORT_AUTOCOMPLETE(BAD_REQUEST),
    OWNER_NOT_FOUND(NOT_FOUND),
    PERMISSION_DENIED(FORBIDDEN),
    UNKNOWN_MIGRATION(NOT_FOUND),
    INVALID_QUERY(BAD_REQUEST),
    IMPLEMENTATION_ABSENT(NOT_FOUND);

    private final int httpCode;

    ErrorCode(Response.Status httpCode) {
        this.httpCode = httpCode.getStatusCode();
    }

    ErrorCode(int httpCode) {
        this.httpCode = httpCode;
    }

    public int getHttpCode() {
        return httpCode;
    }
}
