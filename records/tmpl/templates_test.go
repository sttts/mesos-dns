package tmpl

import (
	"testing"

	"github.com/mesosphere/mesos-dns/records/labels"
)

func TestCompile(t *testing.T) {
	for _, ts := range []struct {
		Template
		rfc labels.Func
		err bool
	}{
		{"abc", labels.RFC952, false},

		{"", labels.RFC952, true},
		{".", labels.RFC952, true},
		{"abc.", labels.RFC952, true},
		{".abc", labels.RFC952, true},
		{".abc.", labels.RFC952, true},
		{".a.b.c.", labels.RFC952, true},
		{"a..bc", labels.RFC952, true},
		{"a...bc", labels.RFC952, true},
		{"1", labels.RFC952, true},
		{"1.2", labels.RFC952, true},
		{"-", labels.RFC952, true},
		{"a-", labels.RFC952, true},
		{"-a", labels.RFC952, true},
		{"a.-.b", labels.RFC952, true},
		{"a:b", labels.RFC952, true},

		{"_abc", labels.RFC952, false},
		{"_{abc}", labels.RFC952, false},
		{"_{abc}._tcp.mesos", labels.RFC952, false},

		{"_", labels.RFC952, true},
		{"a_b", labels.RFC952, true},
		{"abc_", labels.RFC952, true},
		{"_{abc}._", labels.RFC952, true},

		{"abc.def.ghi", labels.RFC952, false},
		{"abc.def123.ghi", labels.RFC952, false},
	} {
		_, err := ts.Compile(ts.rfc)
		if err != nil && !ts.err {
			t.Errorf("cannot compile template %q: %v", ts.Template, err)
			continue
		} else if err == nil && ts.err {
			t.Errorf("expected error compiling template %q", ts.Template)
			continue
		}
	}
}

func TestExecute(t *testing.T) {
	for _, ts := range []struct {
		Template
		rfc     labels.Func
		context Context
		answer  string
		err     bool
	}{
		{"abc", labels.RFC952, Context{}, "abc", false},
		{"abc.def", labels.RFC952, Context{}, "abc.def", false},
		{"abc.def123.ghi.j-k-l", labels.RFC952, Context{}, "abc.def123.ghi.j-k-l", false},

		{"{framework}", labels.RFC952, Context{"framework": "marathon"}, "marathon", false},
		{"{ framework\t}", labels.RFC952, Context{"framework": "marathon"}, "marathon", false},
		{"{   \tframework\t \t}", labels.RFC952, Context{"framework": "marathon"}, "marathon", false},
		{"{framework}.foo", labels.RFC952, Context{"framework": "marathon"}, "marathon.foo", false},
		{"{name}.{framework}", labels.RFC952, Context{"framework": "marathon", "name": "nginx"}, "nginx.marathon", false},
		{"{name}-{framework}", labels.RFC952, Context{"framework": "marathon", "name": "nginx"}, "nginx-marathon", false},
	} {
		compiled, err := ts.Compile(ts.rfc)
		if err != nil {
			t.Errorf("cannot compile template %q: %v", ts.Template, err)
			continue
		}

		got, err := compiled.Execute(ts.context)
		if err != nil && !ts.err {
			t.Errorf("unexpected execution error for template %v in context %v: %v", ts.Template, ts.context, err)
			continue
		} else if err == nil && ts.err {
			t.Errorf("expected execution error for template %v in context %v: got %v", ts.Template, ts.context, got)
			continue
		}

		if got != ts.answer {
			t.Errorf("invalid answer for template %v in context %v: got %q, want %q", ts.Template, ts.context, got, ts.answer)
			continue
		}
	}
}
