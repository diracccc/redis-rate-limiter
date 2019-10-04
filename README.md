# RedLimiter
RedLimiter(**Red**is rate **limiter**)，与Guava RateLimiter功能类似，基于Redis+Lua+令牌桶算法实现的分布式限流器，提供
- 限流
- 流量整形
- 批处理避免Redis热点

## 原理
- 令牌桶算法
- 通过延迟计算方式向令牌桶里加入令牌
- 通过Lua脚本和Redis单线程的特性保证操作的原子性

Lua脚本里提供了整个获取令牌过程。