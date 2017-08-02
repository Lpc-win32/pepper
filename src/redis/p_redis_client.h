/*
 * FileName : p_redis_client.h
 * Author   : Pengcheng Liu(Lpc-Win32)
 * Date     : Thu 27 Jul 2017 05:00:26 PM CST   Created
*/

#pragma once

#include "nocopyable.h"
#include "hiredis.h"

#include <utility>

namespace pepper
{

    class PRedisClient : public nocopyable
    {
        public:
             /*
             * redis命令 2.6.12以上的版本支持
             * SET key value [EX seconds] [PX milliseconds] [NX|XX]
             * return 1 成功 
             *        0 失败
             *        -1 异常
             */
            int set(const std::string &key, const std::string &value);
   
            /*
             * return 设置成功返回1
             *        设置失败返回0
             *        异常 返回 -1
             */
            int setnx(const std::string &key, const std::string &value);
    
            /*
             * 设置成功返回 1
             * 异常返回 -1
             */
            int setex(const std::string &key, uint32_t seconds,
                      const std::string &value);
    
            /*
             * return incr 后的值
             * */
            long long incr(const std::string &key);
            
            /*
             * return 1 成功 
             *        -1 异常
             */
            int mset(const std::vector<std::pair<std::string, std::string> > &fields);
    
            /*
             * return >0 返回的结果数
             *        -1 异常
             */
            int mget(const std::vector<std::string> &keys, std::vector<std::string> &values);
    
            /*
             * return 1 成功
             *        0 没有数据
             *       -1 异常
             */
            int get(const std::string &key, std::string &value);
    
            /*
             * return 1 删除成功
             *        0 key不存在
             *       -1 异常
             */
            int del(const std::string &key);
            int del(const std::vector<std::string> &keys);

            long int dbsize();
    
            /*
             * return 1 设置超时时间成功
             *        0 key不存在
             *       -1 异常
             */
            int expire(const std::string &key, uint32_t secs);
    
            /*
             * @brief 查找所有符合给定模式 pattern 的 key
             * @return -1 错误 >=0 key的数量
             */
            int keys(const std::string &pattern, std::vector<std::string> &out);
    
            /*
             * @brief exists
             * @return 0 不存在
             *         1 存在
             *        -1 redis异常
             */
            int exists(const std::string &key);
            int sadd(const std::string &key, const std::string &value);
            int sadd(const std::string &key, const std::vector<std::string> &value);
    
            /*
             * return 1 成功删除
             *        0 key不存在
             *       -1 异常
             */
            int srem(const std::string &key, const std::string &value);
    
            /*
             * @brief 判断 member 元素是否集合 key 的成员
             * 是集合的元素返回 1
             * 不是集合的元素返回 0
             * 错误返回 -1
             */
            int sismember(const std::string &key, const std::string &value);
    
            int smembers(std::string const &key, std::vector<std::string> &values);
    
            int spop(const std::string &key, std::string &value);
    
            /*
             * @brief 
             * 成功返回0或1   1表示新增key, 0表示覆盖旧值.
             * 失败返回 -1
             */
            int hset(const std::string &key, const std::string &field, const std::string &value);
    
            /*
             * @brief 
             * 成功返回 1
             * 失败返回 0
             * 异常返回 -1
             */
            int hsetnx(const std::string &key, const std::string &field, const std::string &value);
    
            /*
             * @brief 成功返回 1
             * 失败返回 -1
             */
            int hmset(const std::string &key, const std::vector< std::pair<std::string, std::string> > &field_value_pairs);
    
            int hmget(std::string const &key, const std::vector<std::string> &fields, std::vector<std::string> &values);
            /*
             * @brief 成功返回 0 
             * 失败返回 -1
             */
            int hget(const std::string &key, const std::string &field, std::string &value);
    
            /*
             * @brief 成功返回key的个数 >=0
             * 失败返回 -1
             */
            int hgetall(const std::string &key, std::vector< std::pair<std::string, std::string> > &field_value_pairs);
    
            /*
             * @brief key 存在返回 1 不存在返回0
             * 失败返回 -1
             */
            int hexists(const std::string &key, const std::string &field);
    
            /*
             * @brief 成功删除 返回1 删除不存在的key 返回0 
             * 失败返回 -1
             */
            int hdel(const std::string &key, const std::string &field);
            int hdel(const std::string &key, const std::vector<std::string> &fields);
    
            /*
             * @brief 成功返回key的个数
             * 失败返回 -1
             */
            int hkeys(const std::string &key, std::vector<std::string> &out);
    
            /*
             * @brief 成功返回哈希表 key 中所有域的值的个数
             * 失败返回 -1
             */
            int hvals(const std::string &key, std::vector<std::string> &out);
    
            /*
             *@brief 返回哈希表 key 中域的数量。
             * 失败返回 -1
             */
            int hlen(const std::string &key);
    
            /*
             * @brief 在key-filed对应原值的基础上增加value
             * @param res 在命令成功的前提下，返回命令执行结果
             * @return -1 on failed, 1 on success
             */
            int hincrby(const std::string &key, const std::string &field, 
                    const std::string &value, long long &res);
    
            /*
             * @brief lpush
             * return 列表的长度 失败返回 -1
             */
            int lpush(std::string const &key, const std::vector<std::string> &values);
            int lpush(std::string const &key, std::string const &value);
    
            int lpushx(std::string const &key, std::string const &value);
            int rpush(std::string const &key, const std::vector<std::string> &values);
            int rpush(std::string const &key, std::string const &value);
            int llen(std::string const &key);
            int lrange(std::string const &key, int start, int stop,
                    std::vector<std::string> &values);
    
            /*
             * @brief 
             * return 成功返回1  失败返回0
             */
            int lpop(std::string const &key, std::string &value);
            int rpop(std::string const &key, std::string &value);
            int ltrim(std::string const &key, int start, int stop);
    
            int zadd(std::string const &key, int64_t score, std::string const &member);
            int zadd(std::string const &key,const std::vector<std::pair<int64_t, std::string> > &member_score_pair_v);
            int zrem(std::string const &key, std::string const &member);
            int zrem(std::string const &key, std::vector<std::string> const &members);
            int zcard(std::string const &key);
    
            int zscore(const std::string &key, const std::string &member, std::string &score);
    
            /*
             * @brief 
             * return 成功返回元素数量  失败返回0
             */
            int zrangebyscore(std::string const &key, std::string const &min_score,
                    std::string const &max_score,
                    std::vector<std::string> &members);
    
            int zremrangebyscore(std::string const &key,
                    std::string const &min_score, std::string const &max_score);
    
    
            /*
             * @brief 执行多个命令 redis pipeline
             */
            // int exec(std::vector<redis::command> & commands);
    
            /*
             * @brief 查询key的过期时间
             * @return 0 On success, -1 on error
             */
            int ttl(std::string const &key, int &result = s_ignore_ref_params);
    
        private:
            // TODO friend
            PRedisClient() = default;

            bool is_init_ok();

            redisContext *redis_context_;
            redisReply *reply;
    };

}
