package proxy

import (
	"math/rand"
	"pika/codis/v2/pkg/proxy/redis"
	"pika/codis/v2/pkg/utils/sync2/atomic2"
)

var (
	ErrMsgServiceRejected           = "Service is rejected by breaker!"
	breakerSwitch                   atomic2.Int64
	probabilityOfServiceDegradation atomic2.Int64
)

const (
	SWITCH_OPEN          int64 = 1
	SWITCH_CLOSED        int64 = 0
)

// global settings
func BreakerSetState(state int64) {
	if state != 0 && state != 1 {
		state = 0
	}
	breakerSwitch.Set(state)
}

// When Percentage degradation is enabled, set the degradation probability
func BreakerSetProbability(prob int64) {
	if prob < 0 {
		setProbability(0)
	} else if prob > 100 {
		setProbability(100)
	} else {
		setProbability(prob)
	}
}


// Fuse degrade switch
func IsBreakerEnabled() bool {
	return breakerSwitch.Int64() == SWITCH_OPEN
}

// Percentage degradation Interface
func setProbability(prob int64) {
	probabilityOfServiceDegradation.Set(prob)
}
func getProbability() int64 {
	return probabilityOfServiceDegradation.Int64()
}

func IfDegradateService(r *Request, isBigRequest bool, rd *rand.Rand) bool {
	if !IsBreakerEnabled() {
		return false
	}
	// Check CmdBlackList
	if UseCmdBlackList() && IsCmdInBlackList(r) {
			// If use cmdblacklist and exists, degraded
			if ExecuteServiceDegradation(r, rd) { //Execute downgrade strategy
				return true
			}
		}	
	//Check KeyBlackList
	if UseKeyBlackList() && IsKeyInBlackList(r) {
			// If use keyblacklist and exists, degraded
			if ExecuteServiceDegradation(r, rd) {
				return true
			}
		}	
	//Check BigRequest Flag
	if isBigRequest {
		if ExecuteServiceDegradation(r, rd) {
			return true
		}
	}
	// All pass
	return false
}

// Check if invoke CmdBlackList
func UseCmdBlackList() bool {
	return cmdBlackListSwitch.Int64() == SWITCH_OPEN
}

// Check whether the command is a blacklist command
func IsCmdInBlackList(r *Request) bool {
	return CheckCmdBlackList(r.OpStr)
}


// Check if invoke KeyBlackList
func UseKeyBlackList() bool {
	return keyBlackListSwitch.Int64() == SWITCH_OPEN
}

// Check whether the key is a blacklist key
func IsKeyInBlackList(r *Request) bool {
	switch r.OpFlagChecker & (FlagReqKeys | FlagReqKeyValues) {
	case FlagReqKeys:
		if len(r.Multi) < 2 { //Incorrect request
			return false
		}
		for i := 1; i < len(r.Multi); i++ {
			if CheckKeyBlackList(string(r.Multi[i].Value)) { //Reject if in KeyBlackList
				return true
			}
		}
		return false
	case FlagReqKeyValues:
		nkvs := len(r.Multi) - 1
		if nkvs == 0 || nkvs%2 != 0 { //Incorrect request
			return false
		}
		for i := 0; i < nkvs/2; i++ {
			if CheckKeyBlackList(string(r.Multi[i*2+1].Value)) { //Reject if in KeyBlackList
				return true
			}
		}
		return false
	}
	key := getHashKey(r.Multi, r.OpStr)
	return CheckKeyBlackList(string(key))
}

// execute degeration policy;if true,return；if false,cmd go on
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
