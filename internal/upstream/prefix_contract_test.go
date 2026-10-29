package upstream

import "testing"

func TestPick_ShortNameSharingPrefixDoesNotBecomeFullPath(t *testing.T) {
	// Given 短模型名以前缀开头但没有斜杠 When Pick Then 仍按短名匹配
	pkgs, reg := testEnv(t)
	installBuiltinLikePkg(t, pkgs, "demo")
	group := mustGroup(t, reg, "a", "demo", nil)
	mustMember(t, reg, group.ID, "ab", true)
	candidates, _, err := reg.Pick("ab", nil, "openai-completions", false)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("short model did not resolve: candidates=%v err=%v", candidates, err)
	}
	if candidates[0].Member.Name != "ab" || candidates[0].Group.ID != group.ID {
		t.Fatalf("wrong candidate: %+v", candidates[0])
	}
}
