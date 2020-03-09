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

// List contains a number of filter expressions.
type List struct {
	entries map[string]*entry
	// Allowed types.
	types map[string]struct{}
}

type entry struct {
	items []Item
	// Allowed variables.
	vars map[string]struct{}
}

type Item struct {
	Exclude bool
	Expr    ast.Expr
}

func skipSpace(s string) string {
	return strings.TrimLeftFunc(s, unicode.IsSpace)
}

func parseWord(s string) (word, next string) {
	s = skipSpace(s)
	for i, c := range s {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) {
			return s[:i], s[i:]
		}
	}
	return s, ""
}

func isWord(s string) bool {
	for _, c := range s {
		if !unicode.IsLetter(c) && !unicode.IsDigit(c) {
			return false
		}
	}
	return true
}

func (l *List) getEntry(typ string) *entry {
	if l.entries == nil {
		l.entries = map[string]*entry{}
	}
	if l.entries[typ] == nil {
		l.entries[typ] = &entry{}
	}
	return l.entries[typ]
}

// AllowTypes sets the list to allow only specified values as filter types. The
// given strings are included as allowed values. Non-word values are skipped.
func (l *List) AllowTypes(types ...string) *List {
	if l.types == nil {
		l.types = make(map[string]struct{}, len(types))
	}
	for _, t := range types {
		if isWord(t) {
			l.types[t] = struct{}{}
		}
	}
	return l
}

// AllowVars sets the list to allow only specified values as variables for the
// given filter type. The given strings are included as allowed values. Non-word
// values are skipped.
func (l *List) AllowVars(typ string, vars ...string) *List {
	if !isWord(typ) {
		return l
	}
	e := l.getEntry(typ)
	if e.vars == nil {
		e.vars = map[string]struct{}{}
	}
	for _, v := range vars {
		if isWord(v) {
			e.vars[v] = struct{}{}
		}
	}
	return l
}

// Append adds a filter to the list.
func (l *List) Append(filter string) (err error) {
	var item Item
	filter = skipSpace(filter)
	action, filter := parseWord(filter)
	switch action {
	case "include":
		item.Exclude = false
	case "exclude":
		item.Exclude = true
	default:
		return fmt.Errorf("expected include or exclude keyword")
	}

	filter = skipSpace(filter)
	typ, filter := parseWord(filter)
	if typ == "" {
		return fmt.Errorf("expected filter type")
	}
	if l.types != nil {
		if _, ok := l.types[typ]; !ok {
			return fmt.Errorf("invalid filter type %q", typ)
		}
	}

	filter = skipSpace(filter)
	if len(filter) == 0 {
		item.Expr = ast.NewIdent("true")
	} else {
		if !strings.HasPrefix(filter, ":") {
			return fmt.Errorf("expected \":\"")
		}
		filter = filter[1:]
		filter = skipSpace(filter)
		item.Expr, err = parser.ParseExpr(filter)
		if err != nil {
			return fmt.Errorf("expected Go expression: %w", err)
		}
	}
	e := l.getEntry(typ)
	e.items = append(e.items, item)
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

// AsQuery formats the filter type as a SQLite query expression. Literals are
// replaced with parameters, and returned as arguments to be passed to the query
// executor.
//
// The expression is prefixed with the AND operator. If the entry contains no
// items, then the expression is empty.
//
// Variables are prefixed with an underscore.
func (l *List) AsQuery(typ string) (query Query, err error) {
	if l.types != nil {
		if _, ok := l.types[typ]; !ok {
			return Query{}, fmt.Errorf("invalid filter type %q", typ)
		}
	}
	entry, ok := l.entries[typ]
	if !ok || len(entry.items) == 0 {
		return query, nil
	}
	var b strings.Builder
	query.vars = map[string]struct{}{}
	b.WriteString("AND ( ")
	for i := 1; i < len(entry.items); i++ {
		if entry.items[i].Exclude {
			b.WriteString("( ")
		}
	}
	for i, item := range entry.items {
		if i > 0 {
			if item.Exclude {
				b.WriteString(") AND ")
			} else {
				b.WriteString("OR ")
			}
		}
		if item.Exclude {
			b.WriteString("NOT ")
		}
		b.WriteString("( ")
		if err := asQuery(&b, &query.Params, entry.vars, query.vars, item.Expr); err != nil {
			return Query{}, fmt.Errorf("item %s[%d]: %w", typ, i, err)
		}
		b.WriteString(") ")
	}
	b.WriteByte(')')
	query.Expr = b.String()
	return query, nil
}

type Query struct {
	// The query expression.
	Expr string
	// Values of parameters to be passed to the query.
	Params []interface{}
	// Variables that are referenced in the query string.
	vars map[string]struct{}
}

// HasVar reports whether the given variable is referenced with the query.
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
