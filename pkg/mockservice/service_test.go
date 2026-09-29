package mockservice

import "testing"

func TestIdempotentCallProducesOneSideEffect(t *testing.T) {
	s := New()
	key := "k1"

	res1, err := s.Call(key, CallOptions{})
	if err != nil {
		t.Fatal(err)
	}
	res2, err := s.Call(key, CallOptions{}) // 重复请求
	if err != nil {
		t.Fatal(err)
	}
	if res1 != res2 {
		t.Errorf("replayed result differs: %+v vs %+v", res1, res2)
	}
	if got := s.SideEffectCount(key); got != 1 {
		t.Errorf("side effects=%d want 1", got)
	}
}

func TestFlakyThenSuccessUnderRetry(t *testing.T) {
	s := New()
	key := "k2"
	var failed int
	for i := 0; i < 5; i++ {
		_, err := s.Call(key, CallOptions{FailTimes: 2})
		if err != nil {
			failed++
			continue
		}
		break
	}
	if failed != 2 {
		t.Errorf("failures=%d want 2", failed)
	}
	if got := s.SideEffectCount(key); got != 1 {
		t.Errorf("side effects=%d want 1", got)
	}
}

func TestFatalIsRejected(t *testing.T) {
	s := New()
	key := "k3"
	for i := 0; i < 3; i++ {
		if _, err := s.Call(key, CallOptions{Fatal: true}); err == nil {
			t.Fatal("want error on fatal call")
		}
	}
	if got := s.SideEffectCount(key); got != 0 {
		t.Errorf("fatal call produced effects=%d want 0", got)
	}
}
