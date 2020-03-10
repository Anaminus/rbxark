package filter

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// Filter contains a number of rules. Rules are divided into rule sets, each
// denoted by a domain.
type Filter struct {
	ruleSets map[string]*ruleSet
	// Allowed domain names.
	domains map[string]struct{}
}

// A list of rules per domain.
type ruleSet struct {
	// List of rules.
	rules []ruleElement
	// Allowed variable names.
	vars map[string]struct{}
}

// A single rule.
type ruleElement struct {
	Exclude bool
	Expr    ast.Expr
}

// Whether a string contains only letters and digits.
func isWord(s string) bool {
	for _, c := range s {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) {
			return false
		}
	}
	return true
}

// Gets a ruleSet from the given domain.
func (l *Filter) getRuleSet(domain string) *ruleSet {
	if l.ruleSets == nil {
		l.ruleSets = map[string]*ruleSet{}
	}
	if l.ruleSets[domain] == nil {
		l.ruleSets[domain] = &ruleSet{}
	}
	return l.ruleSets[domain]
}

// AllowDomains sets the filter to allow only specified values as domains. The
// given strings are included as allowed values. Non-word values are skipped.
func (l *Filter) AllowDomains(types ...string) *Filter {
	if l.domains == nil {
		l.domains = make(map[string]struct{}, len(types))
	}
	for _, t := range types {
		if isWord(t) {
			l.domains[t] = struct{}{}
		}
	}
	return l
}

// AllowVars sets the list to allow only specified values as variables for the
// given domain. The given strings are included as allowed values. Non-word
// values are skipped.
func (l *Filter) AllowVars(domain string, vars ...string) *Filter {
	if !isWord(domain) {
		return l
	}
	ruleSet := l.getRuleSet(domain)
	if ruleSet.vars == nil {
		ruleSet.vars = map[string]struct{}{}
	}
	for _, v := range vars {
		if isWord(v) {
			ruleSet.vars[v] = struct{}{}
		}
	}
	return l
}

// Trims a string by skipping space characters.
func skipSpace(s string) string {
	return strings.TrimLeftFunc(s, unicode.IsSpace)
}

// Trims a string by parsing a word.
func parseWord(s string) (word, next string) {
	s = skipSpace(s)
	for i, c := range s {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) {
			return s[:i], s[i:]
		}
	}
	return s, ""
}

// Append adds a rule to the filter.
func (l *Filter) Append(rule string) (err error) {
	var r ruleElement
	rule = skipSpace(rule)
	action, rule := parseWord(rule)
	switch action {
	case "include":
		r.Exclude = false
	case "exclude":
		r.Exclude = true
	default:
		return fmt.Errorf("expected include or exclude keyword")
	}

	rule = skipSpace(rule)
	typ, rule := parseWord(rule)
	if typ == "" {
		return fmt.Errorf("expected filter type")
	}
	if l.domains != nil {
		if _, ok := l.domains[typ]; !ok {
			return fmt.Errorf("invalid filter type %q", typ)
		}
	}

	rule = skipSpace(rule)
	if len(rule) == 0 {
		r.Expr = ast.NewIdent("true")
	} else {
		if !strings.HasPrefix(rule, ":") {
			return fmt.Errorf("expected \":\"")
		}
		rule = rule[1:]
		rule = skipSpace(rule)
		r.Expr, err = parser.ParseExpr(rule)
		if err != nil {
			return fmt.Errorf("expected Go expression: %w", err)
		}
	}
	ruleSet := l.getRuleSet(typ)
	ruleSet.rules = append(ruleSet.rules, r)
	return nil
}

func asQuery(b *strings.Builder, args *[]interface{}, vars, used map[string]struct{}, e ast.Expr) error {
	switch e := e.(type) {
	case *ast.BinaryExpr:
		if err := asQuery(b, args, vars, used, e.X); err != nil {
			return fmt.Errorf("left expr: %w", err)
		}
		switch e.Op {
		case token.LAND:
			b.WriteString("AND ")
		case token.LOR:
			b.WriteString("OR ")
		case token.EQL:
			b.WriteString("== ")
		case token.NEQ:
			b.WriteString("!= ")
		// case token.LSS:
		// 	b.WriteString("< ")
		// case token.GTR:
		// 	b.WriteString("> ")
		// case token.LEQ:
		// 	b.WriteString("<= ")
		// case token.GEQ:
		// 	b.WriteString(">= ")
		default:
			return fmt.Errorf("unexpected operator %q", e.Op)
		}
		if err := asQuery(b, args, vars, used, e.Y); err != nil {
			return fmt.Errorf("right expr: %w", err)
		}
	case *ast.ParenExpr:
		b.WriteString("( ")
		if err := asQuery(b, args, vars, used, e.X); err != nil {
			return fmt.Errorf("paren expr: %w", err)
		}
		b.WriteString(") ")

	case *ast.UnaryExpr:
		switch e.Op {
		case token.NOT:
			b.WriteString("NOT ")
		default:
			return fmt.Errorf("unexpected operator %q", e.Op)
		}
		if err := asQuery(b, args, vars, used, e.X); err != nil {
			return fmt.Errorf("unary expr: %w", err)
		}
	case *ast.Ident:
		switch e.Name {
		case "true":
			b.WriteString("TRUE ")
			return nil
		case "false":
			b.WriteString("FALSE ")
			return nil
		case "nil":
			b.WriteString("NULL ")
			return nil
		}
		if vars != nil {
			if _, ok := vars[e.Name]; !ok {
				return fmt.Errorf("unexpected identifier %q", e.Name)
			}
		}
		b.WriteByte('_')
		b.WriteString(e.Name)
		b.WriteByte(' ')
		used[e.Name] = struct{}{}

	case *ast.BasicLit:
		switch e.Kind {
		case token.STRING:
			v, err := strconv.Unquote(e.Value)
			if err != nil {
				return fmt.Errorf("string literal: %w", err)
			}
			*args = append(*args, v)
			b.WriteString("? ")
		default:
			return fmt.Errorf("unexpected literal %s", e.Value)
		}
	}
	return nil
}

// AsQuery formats the rule set specified by the given domain as a SQLite query
// expression. Literals are replaced with parameters, and returned as arguments
// to be passed to the query executor.
//
// The expression is prefixed with the AND operator. If the rule set contains no
// rules, then the expression is empty.
//
// Variables are prefixed with an underscore.
func (l *Filter) AsQuery(domain string) (query Query, err error) {
	if l.domains != nil {
		if _, ok := l.domains[domain]; !ok {
			return Query{}, fmt.Errorf("invalid filter type %q", domain)
		}
	}
	ruleSet, ok := l.ruleSets[domain]
	if !ok || len(ruleSet.rules) == 0 {
		return query, nil
	}
	var b strings.Builder
	query.vars = map[string]struct{}{}
	b.WriteString("AND ( ")
	for i := 1; i < len(ruleSet.rules); i++ {
		if ruleSet.rules[i].Exclude {
			b.WriteString("( ")
		}
	}
	for i, rule := range ruleSet.rules {
		if i > 0 {
			if rule.Exclude {
				b.WriteString(") AND ")
			} else {
				b.WriteString("OR ")
			}
		}
		if rule.Exclude {
			b.WriteString("NOT ")
		}
		b.WriteString("( ")
		if err := asQuery(&b, &query.Params, ruleSet.vars, query.vars, rule.Expr); err != nil {
			return Query{}, fmt.Errorf("item %s[%d]: %w", domain, i, err)
		}
		b.WriteString(") ")
	}
	b.WriteByte(')')
	query.Expr = b.String()
	return query, nil
}

// Query contains the query string and parameters of an SQLite query.
type Query struct {
	// The query expression.
	Expr string
	// Values of parameters to be passed to the query.
	Params []interface{}
	// Variables that are referenced in the query string.
	vars map[string]struct{}
}

// HasVar reports whether the given variable is referenced by the query.
func (expr Query) HasVar(v string) bool {
	_, ok := expr.vars[v]
	return ok
}

// Vars returns a list of variables referenced by the query.
func (expr Query) Vars() []string {
	vars := make([]string, 0, len(expr.vars))
	for v := range expr.vars {
		vars = append(vars, v)
	}
	sort.Strings(vars)
	return vars
}
