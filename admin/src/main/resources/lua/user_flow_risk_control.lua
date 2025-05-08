-- 设置用户访问频率限制的参数
local username = KEYS[1]
local timeWindow = tonumber(ARGV[1]) -- 时间窗口，单位：秒

-- 构造 Redis 中存储用户访问次数的键名
local accessKey = "short-link:user-flow-risk-control:" .. username

-- 原子递增访问次数，并获取递增后的值| accessKey 不存在，Redis 会创建它并设置为 1。
local currentAccessCount = redis.call("INCR", accessKey)

-- 设置键的过期时间（不存在key默认accessKey的值为1，表示新增并设置过期时间）
if currentAccessCount == 1 then
    redis.call("EXPIRE", accessKey, timeWindow)
end

-- 返回当前访问次数
return currentAccessCount

-- 记录用户在指定时间窗口内的访问次数。

-- 每次访问时，递增用户的访问计数。

-- 设置访问计数键的过期时间，确保计数在时间窗口结束后自动重置。

-- 返回当前访问次数，供外部逻辑判断是否超出限制。


-- 1、实现用户请求频率限制：确保某个用户在指定时间窗口内的请求次数不超过设定的阈值。防止恶意刷接口或资源滥用。
-- 2、lua脚本：执行lua脚本是原子性的，避免高并发下因竞态条件导致计数不准确，INCR和EXPIRE必须一起执行，
-- 否则多个请求可能绕过过期时间设置
-- 3、利用用户名构建键名，仅在第一次递增时设置过期时间，保证时间窗口从用户首次请求开始计算，第二次请求直接递增，直到过期后再记录下一个时间窗口