// Package hostlist expands Slurm hostlist expressions such as
// "slurm-c[1-2]" or "gpu[01-03,07],login1" into individual node names.
package hostlist

import (
	"fmt"
	"strconv"
	"strings"
)

// Expand returns every host named by expr, in order.
// Only one bracketed range group per host is supported, which covers the
// names Slurm emits for a flat cluster.
func Expand(expr string) ([]string, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" || expr == "None assigned" {
		return nil, nil
	}
	var hosts []string
	for _, item := range splitTopLevel(expr) {
		open := strings.IndexByte(item, '[')
		if open < 0 {
			hosts = append(hosts, item)
			continue
		}
		closing := strings.IndexByte(item, ']')
		if closing < open {
			return nil, fmt.Errorf("hostlist %q: unbalanced brackets", expr)
		}
		prefix, suffix := item[:open], item[closing+1:]
		for _, part := range strings.Split(item[open+1:closing], ",") {
			lo, hi, found := strings.Cut(part, "-")
			if !found {
				hi = lo
			}
			start, err := strconv.Atoi(lo)
			if err != nil {
				return nil, fmt.Errorf("hostlist %q: %w", expr, err)
			}
			end, err := strconv.Atoi(hi)
			if err != nil {
				return nil, fmt.Errorf("hostlist %q: %w", expr, err)
			}
			if end < start {
				return nil, fmt.Errorf("hostlist %q: descending range %s", expr, part)
			}
			for i := start; i <= end; i++ {
				// Keep zero padding: gpu[01-03] -> gpu01, gpu02, gpu03.
				hosts = append(hosts, fmt.Sprintf("%s%0*d%s", prefix, len(lo), i, suffix))
			}
		}
	}
	return hosts, nil
}

// splitTopLevel splits on commas that are not inside brackets.
func splitTopLevel(s string) []string {
	var parts []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '[':
			depth++
		case ']':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}
