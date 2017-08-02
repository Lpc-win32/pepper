/*
 * FileName : p_redis_client.cpp
 * Author   : Pengcheng Liu(Lpc-Win32)
 * Date     : Fri 28 Jul 2017 02:18:45 PM CST   Created
*/

#include "p_redis_client.h"

#include <libpc/pc_logger.h>

#include <string>
#include <vector>

#include <string.h>

using namespace pepper;

bool PRedisClient::bool is_init_ok()
{
    return redis_context_ != nullptr ? true : false;
}

int PRedisClient::set(const std::string &key, const std::string &value)
{
    if (!is_init_ok()) { return -1 };

    reply = redisCommand(redis_context_, "SET %s %s", key.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("SET %s %s error: reply is nullptr", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("SET %s %s error", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("SET %s %s error: reply type is not REDIS_REPLY_STRING",
                     key.c_str(), value.c_str());
    }

    if (0 != strcmp(reply->str, "OK")) {
        return 0;
    }

    return 1;
}

int PRedisClient::setnx(const std::string &key, const std::string &value)
{
    if (!is_init_ok()) { return -1 };

    reply = redisCommand(redis_context_, "SETNX %s %s", key.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("SETNX %s %s error: reply is nullptr", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("SETNX %s %s error", key.c_str(), value.c_str());
        return -1;
    }

    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("SETNX %s %s error: reply type is not REDIS_REPLY_INTEGER",
                     key.c_str(), value.c_str());
    }

    return static_cast<int>(reply->integer);
}

int PRedisClient::setex(const std::string &key, uint32_t seconds,
                        const std::string &value)
{
    if (!is_init_ok()) { return -1 };

    reply = redisCommand(redis_context_, "SETEX %s %u %s", key.c_str(), seconds, value.c_str());
    if (nullptr == reply) {
        pc_log_error("SETEX %s %u %s error: reply is nullptr", key.c_str(), seconds, value.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("SETEX %s %u %s error", key.c_str(), seconds, value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("SETEX %s %u %s error: reply type is not REDIS_REPLY_STRING",
                     key.c_str(), seconds, value.c_str());
    }

    if (0 != strcmp(reply->str, "OK")) {
        pc_log_error("SETEX %s %u %s error: Return val is not OK", key.c_str(), seconds, value.c_str());
        return 0;
    }

    return 1;
}

long long PRedisClient::incr(const std::string &key)
{
    if (!is_init_ok()) { return -1 };

    reply = redisCommand(redis_context_, "INCR %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("INCR %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("INCR %s error", key.c_str());
        return -1;
    }

    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("SETNX %s error: reply type is not REDIS_REPLY_INTEGER",
                     key.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::mset(const std::vector<std::pair<std::string, std::string> > &fields)
{
    if (!is_init_ok()) { return -1; }

    if (fields.size() % 2 != 0 && fields.size() > 0) {
        pc_log_error("fields is not leagel");
        return -1;
    }
    
    std::string fields_str;
    for (auto field : fields) {
        fields_str += field;
        fields_str += " ";
    }

    reply = redisCommand(redis_context_, "MSET %s", fields_str.c_str());
    if (nullptr == reply) {
        pc_log_error("MSET %s error: reply is nullptr", fields_str.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("MSET %s error", fields_str.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("MSET %s error: type is not REDIS_REPLY_STRING", fields_str.c_str());
        return -1;
    }
    if (0 != strcmp(reply->str, "OK")) {
        pc_log_error("MSET %s error: return val is not \"OK\"");
        return -1;
    }
    return 0;
}

int PRedisClient::mget(const std::vector<std::string> &keys, std::vector<std::string> &values)
{
    if (!is_init_ok()) { return -1; }

    if (keys.size() % 2 != 0 && keys.size() > 0) {
        pc_log_error("keys is not leagel");
        return -1;
    }

    std::string keys_str;
    for (auto key : keys) {
        keys_str += key;
        keys_str += " ";
    }

    reply = redisCommand(redis_context_, "MGET %s", keys_str.c_str());
    if (nullptr == reply) {
        pc_log_error("MGET %s error: reply is nullptr", keys_str.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("MGET %s error", keys_str.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        pc_log_error("MGET %s error: type is not REDIS_REPLY_ARRAY", keys_str.c_str());
        return -1;
    }
    if (reply->elements < 1) {
        pc_log_error("MGET %s error: elements is less than 1", keys_str.c_str());
        return 0;
    }

    for (int i = 0 ; i < reply->elements; i++) {
        if (reply->element[i]->type != REDIS_REPLY_STRING) {
            values.push_back("");
        }
        values.push_back(reply->element[i]->str);
    }

    return values.size();
}

int PRedisClient::get(const std::string &key, std::string &value)
{
    if (!is_init_ok()) { return -1; }
    
    reply = redisCommand(redis_context_, "GET %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("GET %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("GET %s error", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        pc_log_info("GET %s info: (nil)", key.c_str());
        return 0;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("Get %s error: type is not REDIS_REPLY_STRING", key.c_str());
        return -1;
    }
    value = reply->str;

    return 0;
}

int PRedisClient::del(const std::string &key)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("DEL %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("DEL %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("DEL %s error", key.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("DEL %s error: type is not REDIS_REPLY_INTEGER", key.c_str());
        return -1;
    }
    return reply->integer;
}

int PRedisClient::del(const std::vector<std::string> &keys)
{
    if (!is_init_ok()) { return -1; }

    std::string keys_str;
    for (auto key : keys) {
        keys_str += key;
        keys_str += " ";
    }

    reply = redisCommand("DEL %s", keys_str.c_str());
    if (nullptr == reply) {
        pc_log_error("DEL %s error: reply is nullptr", keys_str.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("DEL %s error", keys_str.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("DEL %s error: type is not REDIS_REPLY_INTEGER", keys_str.c_str());
        return -1;
    }
    return reply->integer;
}

long int PRedisClient::dbsize()
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("DBSIZE");
    if (nullptr == reply) {
        pc_log_error("DBSIZE error: reply is nullptr");
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("DBSIZE error");
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("DBSIZE error: type is not REDIS_REPLY_INTEGER");
        return -1;
    }
    return reply->integer;
}

int PRedisClient::expire(const std::string &key, uint32_t secs)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("EXPIRE %s %u", key.c_str(), secs);
    if (nullptr == reply) {
        pc_log_error("EXPIRE %s %u error: reply is nullptr", key.c_str(), secs);
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("EXPIRE %s %u error", key.c_str(), secs);
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("EXPIRE %s %u error: type is not REDIS_REPLY_INTEGER");
        return -1;
    }
    return reply->integer;
}

int PRedisClient::keys(const std::string &pattern, std::vector<std::string> &out)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("KEYS %s", pattern.c_str());
    if (nullptr == reply) {
        pc_log_error("KEYS %s error: reply is nullptr", pattern.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("KEYS %s error", pattern.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        pc_log_error("KEYS %s error: type is not REDIS_REPLY_ARRAY", pattern.c_str());
        return -1;
    }

    for (int i = 0; i < reply->elements; i++) {
        out.push_back(reply->element[i]->str);
    }

    return reply->elements;
}

int PRedisClient::exists(const std::string &key)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("EXISTS %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("EXISTS %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("EXISTS %s error", key.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("EXISTS %s error: type is not REDIS_REPLY_INTEGER", key.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::sadd(const std::string &key, const std::string &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("SADD %s %s", key.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("SADD %s %s error: reply is nullptr", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("SADD %s %s error", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("SADD %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), value.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::sadd(const std::string &key, const std::vector<std::string> &values)
{
    if (!is_init_ok()) { return -1; }

    std::string value_str;
    for (auto value : values) {
        value_str += value;
        value_str += " ";
    }

    reply = redisCommand("SADD %s %s", key.c_str(), value_str.c_str());
    if (nullptr == reply) {
        pc_log_error("SADD %s %s error: reply is nullptr", key.c_str(), value_str.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("SADD %s %s error", key.c_str(), value_str.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("SADD %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), value_str.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::srem(const std::string &key, const std::string &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("SREM %s %s", key.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("SREM %s %s error: reply is nullptr", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("SREM %s %s error", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("SREM %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), value.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::sismember(const std::string &key, const std::string &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("SISMEMBER %s %s", key.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("SISMEMBER %s %s error: reply is nullptr", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("SISMEMBER %s %s error", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("SISMEMBER %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), value.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::smembers(std::string const &key, std::vector<std::string> &values)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("SMEMBERS %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("SMEMBERS %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("SMEMBERS %s error", key.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        pc_log_error("SMEMBERS %s error: type is not REDIS_REPLY_ARRAY", key.c_str());
        return -1;
    }

    for (int i = 0; i < reply->elements; i++) {
        values.push_back(reply->element[i]->str);
    }

    return reply->elements;
}

int PRedisClient::spop(const std::string &key, std::string &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("SPOP %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("SPOP %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("SPOP %s error", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        pc_log_info("SPOP %s info: type is REDIS_REPLY_NIL");
        return 0;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("SPOP %s error: type is not REDIS_REPLY_STRING", key.c_str());
        return -1;
    }

    value = reply->str;

    return 1;
}

int PRedisClient::hset(const std::string &key, const std::string &field, const std::string &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("HSET %s %s %s error: reply is nullptr", key.c_str(), field.c_str(), value.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HSET %s %s %s error", key.c_str(), field.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("HSET %s %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), field.c_str(), value.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::hsetnx(const std::string &key, const std::string &field, const std::string &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("HSETNX %s %s %s", key.c_str(), field.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("HSETNX %s %s %s error: reply is nullptr", key.c_str(), field.c_str(), value.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HSETNX %s %s %s error", key.c_str(), field.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("HSETNX %s %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), field.c_str(), value.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::hmset(const std::string &key, const std::vector< std::pair<std::string, std::string> > &field_value_pairs)
{
    if (!is_init_ok()) { return -1; }

    std::string redis_msg;
    for (auto field : field_value_pairs) {
        redis_msg += field.first;
        redis_msg += " ";
        redis_msg += field.second;
        redis_msg += " ";
    }  

    reply = redisCommand("HMSET %s %s", key.c_str(), redis_msg.c_str());
    if (nullptr == reply) {
        pc_log_error("HMSET %s %s error: reply is nullptr", key.c_str(), redis_msg.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HMSET %s %s %s error", key.c_str(), redis_msg.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("HMSET %s %s error: type is not REDIS_REPLY_STRING", key.c_str(), redis_msg.c_str());
        return -1;
    }
    if (0 != strcmp("OK", reply->str)) {
        return 0;
    }
    
    return 1;
}

int PRedisClient::hmget(std::string const &key, const std::vector<std::string> &fields, std::vector<std::string> &values)
{
    if (!is_init_ok()) { return -1; }

    std::string redis_msg;
    for (auto field : fields) {
        redis_msg += field;
        redis_msg += " ";
    }

    reply = redisCommand("HMGET %s %s", key.c_st(), redis_msg.c_str());
    if (nullptr == reply) {
        pc_log_error("HMGET %s %s error: reply is nullptr", key.c_str(), redis_msg.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HMSET %s %s %s error", key.c_str(), redis_msg.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        pc_log_error("HMSET %s %s error: type is not REDIS_REPLY_ARRAY", key.c_str(), redis_msg.c_str());
        return -1;
    }
    for (int i = 0; i < reply->elements; i++) {
        values.push_back(reply->element[i]->str);
    }
    return reply->elements;
}

int PRedisClient::hget(const std::string &key, const std::string &field, std::string &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("HGET %s %s", key.c_str(), field.c_str());
    if (nullptr == reply) {
        pc_log_error("HGET %s %s error: reply is nullptr", key.c_str(), field.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HGET %s %s error", key.c_str(), field.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        return 0;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("HGET %s %s error: type is not REDIS_REPLY_STRING", key.c_str(), field.c_str());
        return -1;
    }
    value = reply->str;

    return 1;
}

int PRedisClient::hgetall(const std::string &key, std::vector< std::pair<std::string, std::string> > &field_value_pairs)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("HGETALL %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("HGETALL %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HGETALL %s error", key.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        pc_log_error("HGETALL %s error: type is not REDIS_REPLY_ARRAY", key.c_str());
        return -1;
    }
    if (reply->elements % 2 != 0) {
        pc_log_error("HGETALL %s error: elements is %d", key.c_str(), reply->elements);
        return -1;
    }
    for (int i = 0; i < reply->elements; i += 2) {
        std::pair<std::string, std::string> value_pair;
        value_pair.first  = reply->element[i]->str;
        value_pair.second = reply->element[i+1]->str;
    }

    return reply->elements;
}

int PRedisClient::hexists(const std::string &key, const std::string &field)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("HEXISTS %s %s", key.c_str(), field.c_str());
    if (nullptr == reply) {
        pc_log_error("HEXISTS %s %s error: reply is nullptr", key.c_str(), field.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HSET %s %s error", key.c_str(), field.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("HSET %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), field.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::hdel(const std::string &key, const std::string &field)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("HDEL %s %s", key.c_str(), field.c_str());
    if (nullptr == reply) {
        pc_log_error("HDEL %s %s error: reply is nullptr", key.c_str(), field.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HDEL %s %s error", key.c_str(), field.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("HDEL %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), field.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::hdel(const std::string &key, const std::vector<std::string> &fields)
{
    if (!is_init_ok()) { return -1; }

    std::string field;
    for (auto field_ : fields) {
        field += field_;
        field += " ";
    }

    reply = redisCommand("HDEL %s %s", key.c_str(), field.c_str());
    if (nullptr == reply) {
        pc_log_error("HDEL %s %s error: reply is nullptr", key.c_str(), field.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HDEL %s %s error", key.c_str(), field.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("HDEL %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), field.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::hkeys(const std::string &key, std::vector<std::string> &out)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("HKEYS %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("HKEYS %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HKEYS %s error", key.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        pc_log_error("HKEYS %s error: type is not REDIS_REPLY_ARRAY", key.c_str());
        return -1;
    }
    for (int i = 0; i < reply->elements; ++i) {
        out.push_back(reply->element[i]->str);
    }

    return reply->elements;
}

int PRedisClient::hvals(const std::string &key, std::vector<std::string> &out)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("HVALS %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("HVALS %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HVALS %s error", key.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        pc_log_error("HVALS %s error: type is not REDIS_REPLY_ARRAY", key.c_str());
        return -1;
    }
    for (int i = 0; i < reply->elements; ++i) {
        out.push_back(reply->element[i]->str);
    }

    return reply->elements;
}

int PRedisClient::hlen(const std::string &key)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("HLEN %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("HLEN %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HLEN %s error", key.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("HLEN %s error: type is not REDIS_REPLY_INTEGER", key.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::hincrby(const std::string &key, const std::string &field,
            const std::string &value, long long &res)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("HINCRBY %s %s %s", key.c_str(), field.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("HINCRBY %s %s %s error: reply is nullptr", key.c_str(), field.c_str(), value.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HLEN %s error", key.c_str(), field.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("HLEN %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), field.c_str(), value.c_str());
        return -1;
    }
    res = reply->integer;

    return 1;
}

int PRedisClient::lpush(std::string const &key, std::string const &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("LPUSH %s %s", key.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("LPUSH %s %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("LPUSH %s %s error", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("LPUSH %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), value.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::lpush(std::string const &key, const std::vector<std::string> &values)
{
    if (!is_init_ok()) { return -1; }

    std::string value;
    for (auto value_ : values) {
        value += value_;
        value += " ";
    }

    reply = redisCommand("LPUSH %s %s", key.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("LPUSH %s %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("LPUSH %s %s error", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("LPUSH %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), value.c_str());
        return -1;
    }

    return reply->integer;  
}

int PRedisClient::lpushx(std::string const &key, std::string const &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("LPUSHX %s %s", key.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("LPUSHX %s %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("LPUSHX %s %s error", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("LPUSHX %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), value.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::rpush(std::string const &key, std::string const &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("RPUSH %s %s", key.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("RPUSH %s %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("RPUSH %s %s error", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("RPUSH %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), value.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::rpush(std::string const &key, const std::vector<std::string> &values)
{
    if (!is_init_ok()) { return -1; }

    std::string value;
    for (auto value_ : values) {
        value += value_;
        value += " ";
    }

    reply = redisCommand("RPUSH %s %s", key.c_str(), value.c_str());
    if (nullptr == reply) {
        pc_log_error("RPUSH %s %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("RPUSH %s %s error", key.c_str(), value.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("RPUSH %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), value.c_str());
        return -1;
    }

    return reply->integer;  
}

int PRedisClient::llen(const std::string &key)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("LLEN %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("LLEN %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("LLEN %s error", key.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("LLEN %s error: type is not REDIS_REPLY_INTEGER", key.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::lrange(std::string const &key, int start, int stop,
                         std::vector<std::string> &values)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("LRANGE %s %d %d", key.c_str(), start, stop);
    if (nullptr == reply) {
        pc_log_error("LRANGE %s %d %d error: reply is nullptr", key.c_str(), start, stop);
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("LRANGE %s %d %d error", key.c_str(), start, stop);
        return -1;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        pc_log_error("LRANGE %s %d %d error: reply is nullptr", key.c_str(), start, stop);
        return -1;
    }

    for (int i = 0; i < reply->elements; ++i) {
        values.push_back(reply->element[i]->str);
    }

    return reply->elements;
}

int PRedisClient::lpop(std::string const &key, std::string &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("LPOP %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("LPOP %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HGET %s error", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        return 0;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("LPOP %s error: type is not REDIS_REPLY_STRING", key.c_str());
        return -1;
    }
    value = reply->str;

    return 1;
}

int PRedisClient::rpop(std::string const &key, std::string &value)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("LPOP %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("LPOP %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("HGET %s error", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_NIL) {
        return 0;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("LPOP %s error: type is not REDIS_REPLY_STRING", key.c_str());
        return -1;
    }
    value = reply->str;

    return 1;
}

int PRedisClient::ltrim(std::string const &key, int start, int stop)
{
    if (!is_init_ok()) { return -1 };

    reply = redisCommand(redis_context_, "LTRIM %s %d %d", key.c_str(), start, stop);
    if (nullptr == reply) {
        pc_log_error("LTRIM %s %d %d error: reply is nullptr", key.c_str(), start, stop);
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("LTRIM %s %d %d error", key.c_str(), start, stop);
        return -1;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("LTRIM %s %d %d error: reply type is not REDIS_REPLY_STRING",
                     key.c_str(), start, stop);
    }

    if (0 != strcmp(reply->str, "OK")) {
        return 0;
    }

    return 1;
}

int PRedisClient::zadd(std::string const &key, int64_t score, std::string const &member)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("ZADD %s %ld %s", key.c_str(), score, member.c_str());
    if (nullptr == reply) {
        pc_log_error("ZADD %s %ld %s error: reply is nullptr", key.c_str(), score, member.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("ZADD %s %ld %s error", key.c_str(), score, member.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("ZADD %s %ld %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), score, member.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::zadd(std::string const &key,const std::vector<std::pair<int64_t, std::string> > &member_score_pair_v)
{
    if (!is_init_ok()) { return -1; }

    std::string score_member_str;
    for (auto score_member : member_score_pair_v) {
        score_member_str += score_member.first;
        score_member_str += " ";
        score_member_str += score_member.second;
        score_member_str += " ";
    }

    reply = redisCommand("ZADD %s %s", key.c_str(), score_member_str.c_str());
    if (nullptr == reply) {
        pc_log_error("ZADD %s %s error: reply is nullptr", key.c_str(), score_member_str.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("ZADD %s %s error", key.c_str(), score_member_str.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("ZADD %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), score_member_str.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::zrem(std::string const &key, std::string const &member)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("ZREM %s %s", key.c_str(), member.c_str());
    if (nullptr == reply) {
        pc_log_error("ZREM %s %s error: reply is nullptr", key.c_str(), member.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("ZREM %s %s error", key.c_str(), member.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("ZREM %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), member.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::zrem(std::string const &key, std::vector<std::string> const &members)
{
    if (!is_init_ok()) { return -1; }

    std::string member;
    for (auto member_ : members) {
        member += member_;
        member += " ";
    }

    reply = redisCommand("ZREM %s %s", key.c_str(), member.c_str());
    if (nullptr == reply) {
        pc_log_error("ZREM %s %s error: reply is nullptr", key.c_str(), member.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("ZREM %s %s error", key.c_str(), member.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("ZREM %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), member.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::zcard(std::string const &key)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("ZCARD %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("ZREM %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("ZREM %s error", key.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("ZREM %s error: type is not REDIS_REPLY_INTEGER", key.c_str());
        return -1;
    }

    return reply->integer;
}

int PRedisClient::zscore(const std::string &key, const std::string &member, std::string &score)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("ZSCORE %s %s", key.c_str(), member.c_str());
    if (nullptr == reply) {
        pc_log_error("ZSCORE %s %s error: reply is nullptr", key.c_str(), member.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("ZSCORE %s %s error", key.c_str(), member.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_STRING) {
        pc_log_error("ZSCORE %s %s error: type is not REDIS_REPLY_STRING", key.c_str(), member.c_str());
        return -1;
    }
    score = reply->str;

    return 1;
}

int PRedisClient::zrangebyscore(std::string const &key, std::string const &min_score,
        std::string const &max_score, std::vector<std::string> &members)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("ZRANGEBYSCORE %s %s %s", key.c_str(), min_score.c_str(), max_score.c_str());
    if (nullptr == reply) {
        pc_log_error("ZRANGEBYSCORE %s %s %s error: reply is nullptr", key.c_str(), min_score.c_str(), max_score.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("ZRANGEBYSCORE %s %s %s error", key.c_str(), min_score.c_str(), max_score.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        pc_log_error("ZRANGEBYSCORE %s %s %s error: type is not REDIS_REPLY_ARRAY", key.c_str(), min_score.c_str(), max_score.c_str());
        return -1;
    }
    for (int i = 0; i < reply->elements; ++i) {
        members.push_back(reply->element[i]->str);
    }

    return 1;
}

int PRedisClient::zremrangebyscore(std::string const &key, std::string const &min_score, std::string const &max_score)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("ZREMRANGEBYSCORE %s %s %s", key.c_str(), min_score.c_str(), max_score.c_str());
    if (nullptr == reply) {
        pc_log_error("ZREMRANGEBYSCORE %s %s %s error: reply is nullptr", key.c_str(), min_score.c_str(), max_score.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("ZREMRANGEBYSCORE %s %s %s error", key.c_str(), min_score.c_str(), max_score.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("ZREMRANGEBYSCORE %s %s %s error: type is not REDIS_REPLY_INTEGER", key.c_str(), min_score.c_str(), max_score.c_str());
        return -1;
    }

    return reply->integer;
}

int ttl(std::string const &key, int &result)
{
    if (!is_init_ok()) { return -1; }

    reply = redisCommand("TTL %s", key.c_str());
    if (nullptr == reply) {
        pc_log_error("TTL %s error: reply is nullptr", key.c_str());
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        pc_log_error("TTL %s error", key.c_str());
        return -1;
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
        pc_log_error("TTL %s error: type is not REDIS_REPLY_INTEGER", key.c_str());
        return -1;
    }
    result = reply->integer;

    return 0;
}
