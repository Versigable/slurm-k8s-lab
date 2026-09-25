package hostlist

import (
	"slices"
	"testing"
)

func TestExpand(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"None assigned", nil},
		{"slurm-c1", []string{"slurm-c1"}},
		{"slurm-c[1-2]", []string{"slurm-c1", "slurm-c2"}},
		{"gpu[01-03,07]", []string{"gpu01", "gpu02", "gpu03", "gpu07"}},
		{"gpu[1-2],login1", []string{"gpu1", "gpu2", "login1"}},
		{"rack[3-4]-a", []string{"rack3-a", "rack4-a"}},
	}
	for _, tt := range tests {
		got, err := Expand(tt.in)
		if err != nil {
			t.Fatalf("Expand(%q): %v", tt.in, err)
		}
		if !slices.Equal(got, tt.want) {
			t.Errorf("Expand(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestExpandErrors(t *testing.T) {
	for _, in := range []string{"gpu]1[", "gpu[a-b]", "gpu[5-2]"} {
		if _, err := Expand(in); err == nil {
			t.Errorf("Expand(%q): expected an error", in)
		}
	}
}
