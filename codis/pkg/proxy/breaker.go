package proxy

import (
	"math/rand"
	"pika/codis/v2/pkg/proxy/redis"
	"pika/codis/v2/pkg/utils/sync2/atomic2"
	"time"

	"golang.org/x/time/rate"
)

var (
	ErrMsgServiceRejected           = "Service is rejected by breaker!"
	limiterState                    atomic2.Int64
	breakerSwitch                   atomic2.Int64
	probabilityOfServiceDegradation atomic2.Int64
	limiter                         *rate.Limiter
)

const (
	BUCKET_FILL_INTERVAL int64 = 10 // 令牌桶填充间隔，以一秒为单位进行划分，比如值为10，qps为10000，意味着间隔为1s/10 = 100ms，桶容量为10000/10 = 1000
	LIMITER_QPS_DEFAULT  int64 = 1000
	SWITCH_OPEN          int64 = 1
	SWITCH_CLOSED        int64 = 0
)

// 全局设置
func BreakerSetState(state int64) {
	if state != 0 && state != 1 { //传入参数不是1或0
		state = 0 //强制置为0，即关闭
	}
	breakerSwitch.Set(state)
}

// 百分比降级时，概率设置
func BreakerSetProbability(prob int64) {
	if prob < 0 {
		setProbability(0)
	} else if prob > 100 {
		setProbability(100)
	} else {
		setProbability(prob)
	}
}

// 初始化令牌桶（限流器）
func BreakerNewQpsLimiter(qps int64) {
	// 根据qps计算令牌桶的填充速率, 速率是向下取整，所以要求qps尽量为10的整数倍
	var limit rate.Limit
	switch {
	case qps <= 0: //小于等于0意味着关闭限流器，即不设上限
		qps = 0
		limiterState.Set(SWITCH_CLOSED)
		limit = rate.Every(time.Millisecond)
	case qps > 0 && qps <= 1e9:
		limiterState.Set(SWITCH_OPEN)
		limit = rate.Every(time.Nanosecond * time.Duration(1e9/qps))
	default:
		limiterState.Set(SWITCH_OPEN)
		qps = LIMITER_QPS_DEFAULT
		limit = rate.Every(time.Millisecond)
	}
	limiter = rate.NewLimiter(limit, int(qps/BUCKET_FILL_INTERVAL))
}

// 令牌桶设置
func BreakerSetTokenBucket(qps int64) {
	// 根据qps计算令牌桶的填充速率, 速率是向下取整，所以要求qps尽量为10的整数倍
	var limit rate.Limit
	switch {
	case qps <= 0: //小于等于0意味着关闭限流器，即不设上限
		qps = 0
		limiterState.Set(SWITCH_CLOSED)
		limit = rate.Every(time.Millisecond)
	case qps > 0 && qps <= 1e9:
		limiterState.Set(SWITCH_OPEN)
		limit = rate.Every(time.Nanosecond * time.Duration(1e9/qps))
	default:
		limiterState.Set(SWITCH_OPEN)
		qps = LIMITER_QPS_DEFAULT
		limit = rate.Every(time.Millisecond)
	}
	limiter.SetLimit(limit)
	limiter.SetBurst(int(qps / BUCKET_FILL_INTERVAL))
}

// 熔断降级开关
func IsBreakerEnabled() bool {
	return breakerSwitch.Int64() == SWITCH_OPEN
}

// 按百分比降级时，阈值设置接口
func setProbability(prob int64) {
	probabilityOfServiceDegradation.Set(prob)
}
func getProbability() int64 {
	return probabilityOfServiceDegradation.Int64()
}

// 令牌桶操作
func isAllowed() bool {
	if limiterState.Int64() == SWITCH_CLOSED { // 限流器关闭状态，直接返回true，允许请求通过
		return true
	}
	return limiter.Allow()
}

// 是否降级
func IfDegradateService(r *Request, isBigRequest bool, rd *rand.Rand) bool {
	if !IsBreakerEnabled() {
		return false
	}
	// [检查项1] 检查命令的黑白名单
	if UseCmdWhiteList() { // 如果选择使用白名单过滤
		if !IsCmdInWhiteList(r) {
			// 白名单中没有，则就对其进行降级处理
			if ExecuteServiceDegradation(r, rd) { //执行降级策略
				return true
			}
		}
	} else {
		// 否则走黑名单方式过滤
		if UseCmdBlackList() && IsCmdInBlackList(r) {
			// 确认使用黑名单且黑名单中有，则就对其进行降级处理
			if ExecuteServiceDegradation(r, rd) { //执行降级策略
				return true
			}
		}
	}

	// [检查项2] 检查key的黑白名单
	if UseKeyWhiteList() { // 如果选择使用白名单过滤
		if !IsKeyInWhiteList(r) {
			// 白名单中没有，则就对其进行降级处理
			if ExecuteServiceDegradation(r, rd) { //执行降级策略
				return true
			}
		}
	} else {
		if UseKeyBlackList() && IsKeyInBlackList(r) {
			// 确认使用黑名单且黑名单中有，则就对其进行降级处理
			if ExecuteServiceDegradation(r, rd) { //执行降级策略
				return true
			}
		}
	}

	// [检查项3] 执行大请求检测
	if isBigRequest {
		if ExecuteServiceDegradation(r, rd) { //执行降级策略
			return true
		}
	}

	// [检查项4] 限流检测
	if IsExceedQpsLimitation(r) {
		// 流量超出限制，直接降级
		return true
	}

	// 所有检测通过
	return false
}

// 判断Qps是否超出
func IsExceedQpsLimitation(r *Request) bool {
	if !isAllowed() {
		r.Resp = redis.NewErrorf(ErrMsgServiceRejected)
		return true
	}
	return false
}

// 判断是否调用命令白名单
func UseCmdWhiteList() bool {
	return cmdWhiteListSwitch.Int64() == SWITCH_OPEN
}

// 判断是否为白名单命令
func IsCmdInWhiteList(r *Request) bool {
	return CheckCmdWhiteList(r.OpStr)
}

// 判断是否调用命令黑名单
func UseCmdBlackList() bool {
	return cmdBlackListSwitch.Int64() == SWITCH_OPEN
}

// 判断是否为黑名单命令
func IsCmdInBlackList(r *Request) bool {
	return CheckCmdBlackList(r.OpStr)
}

// 判断是否调用key白名单
func UseKeyWhiteList() bool {
	return keyWhiteListSwitch.Int64() == SWITCH_OPEN
}

// 判断是否为白名单key
func IsKeyInWhiteList(r *Request) bool {
	switch r.OpFlagMonitor & (FlagReqKeys | FlagReqKeyValues) {
	case FlagReqKeys:
		if len(r.Multi) < 2 { //非正确请求
			return false
		}
		for i := 1; i < len(r.Multi); i++ {
			if !CheckKeyWhiteList(string(r.Multi[i].Value)) { // 只要出现不在白名单的key，就拒绝该命令
				return false
			}
		}
		return true
	case FlagReqKeyValues:
		nkvs := len(r.Multi) - 1
		if nkvs == 0 || nkvs%2 != 0 { //非正确请求
			return false
		}
		for i := 0; i < nkvs/2; i++ {
			if !CheckKeyWhiteList(string(r.Multi[i*2+1].Value)) { // 只要出现不在白名单的key，就拒绝该命令
				return false
			}
		}
		return true
	}
	key := getHashKey(r.Multi, r.OpStr)
	return CheckKeyWhiteList(string(key))
}

// 判断是否调用key黑名单
func UseKeyBlackList() bool {
	return keyBlackListSwitch.Int64() == SWITCH_OPEN
}

// 判断是否为黑名单key
func IsKeyInBlackList(r *Request) bool {
	switch r.OpFlagMonitor & (FlagReqKeys | FlagReqKeyValues) {
	case FlagReqKeys:
		if len(r.Multi) < 2 { //非正确请求
			return false
		}
		for i := 1; i < len(r.Multi); i++ {
			if CheckKeyBlackList(string(r.Multi[i].Value)) { //只要在黑名单中，就拒绝
				return true
			}
		}
		return false
	case FlagReqKeyValues:
		nkvs := len(r.Multi) - 1
		if nkvs == 0 || nkvs%2 != 0 { //非正确请求
			return false
		}
		for i := 0; i < nkvs/2; i++ {
			if CheckKeyBlackList(string(r.Multi[i*2+1].Value)) { //只要在黑名单中，就拒绝
				return true
			}
		}
		return false
	}
	key := getHashKey(r.Multi, r.OpStr)
	return CheckKeyBlackList(string(key))
}

// 执行服务降级策略，为true表示确定降级，直接返回；为false表示某些策略下不降级，继续放行操作
func ExecuteServiceDegradation(r *Request, rd *rand.Rand) bool {
	return rejectServiceOnProbabilty(r, rd)
}

func rejectServiceOnProbabilty(r *Request, rd *rand.Rand) bool {
	if rd.Intn(100) < int(getProbability()) {
		r.Resp = redis.NewErrorf(ErrMsgServiceRejected)
		return true
	}
	return false
}
