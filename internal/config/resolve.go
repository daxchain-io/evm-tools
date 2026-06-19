package config

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// resolve applies value interpolation and command execution to a settings
// subset (shared keys plus one tool's subtree), in place, before strict
// decoding. See docs/design.md "Value interpolation".
//
// Order matches the design: ${...} interpolation runs first on every string
// (a _cmd command string is interpolated lazily, only if it is actually run);
// then _cmd keys are resolved. Command execution is opt-in via allowExec — any
// _cmd key while exec is disabled is a fatal error, not a silent skip. The
// subset only ever contains the current tool's section, so a _cmd in the
// sibling tool's section is ignored along with the rest of that section.
func (l *Loader) resolve(subset map[string]any, allowExec bool) error {
	if !allowExec {
		if p := firstCmdKey(subset, ""); p != "" {
			return fmt.Errorf("config key %q uses command execution, which is disabled; pass --allow-exec or set EVM_TOOLS_ALLOW_EXEC=1", p)
		}
	}
	if err := l.interpolateValue("", subset); err != nil {
		return err
	}
	if allowExec {
		if err := l.resolveCmd(subset, "", false); err != nil {
			return err
		}
	}
	return nil
}

// interpolateValue expands environment references in every string reachable
// from v, mutating maps and slices in place. Keys ending in "_cmd" are left
// untouched here and interpolated lazily at execution time, so a command that
// would be short-circuited by a higher-precedence binding never fails on an
// unset variable it references.
func (l *Loader) interpolateValue(path string, v any) error {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			if strings.HasSuffix(k, cmdSuffix) {
				continue
			}
			child := joinPath(path, k)
			if s, ok := val.(string); ok {
				// Interpolation applies to file-sourced values only; a value
				// that arrives via a flag/env binding is left untouched.
				if l.isBindingOverride(child) {
					continue
				}
				out, err := expandString(s)
				if err != nil {
					return fmt.Errorf("%s: %w", child, err)
				}
				x[k] = out
				continue
			}
			if err := l.interpolateValue(child, val); err != nil {
				return err
			}
		}
		return nil
	case []any:
		for i, val := range x {
			if s, ok := val.(string); ok {
				out, err := expandString(s)
				if err != nil {
					return fmt.Errorf("%s[%d]: %w", labelOf(path), i, err)
				}
				x[i] = out
				continue
			}
			if err := l.interpolateValue(fmt.Sprintf("%s[%d]", path, i), val); err != nil {
				return err
			}
		}
		return nil
	default:
		return nil
	}
}

// isBindingOverride reports whether dottedPath is supplied by a flag the user
// changed or by an EVM_TOOLS_ environment variable — a source that outranks the
// config file. Built-in defaults are deliberately excluded (viper.IsSet would
// count them), so a _cmd on a field that merely has a default still runs, and a
// file value with a default is still interpolated.
func (l *Loader) isBindingOverride(dottedPath string) bool {
	if dottedPath == "" {
		return false
	}
	if l.flagKeys[dottedPath] {
		return true
	}
	envKey := EnvPrefix + "_" + strings.ToUpper(strings.ReplaceAll(dottedPath, ".", "_"))
	_, ok := os.LookupEnv(envKey)
	return ok
}

const cmdSuffix = "_cmd"

// resolveCmd replaces every "<field>_cmd" key with "<field>" set to the trimmed
// stdout of the command. inArray is true once recursion is inside an
// array-of-tables element, where keys are not addressable as dotted Viper keys
// (and bindings never target them), so sibling detection is purely structural.
func (l *Loader) resolveCmd(node map[string]any, path string, inArray bool) error {
	cmdKeys := make([]string, 0)
	for k, v := range node {
		if !strings.HasSuffix(k, cmdSuffix) {
			continue
		}
		if _, ok := v.(string); !ok {
			return fmt.Errorf("%s: _cmd value must be a string", joinPath(path, k))
		}
		cmdKeys = append(cmdKeys, k)
	}

	for _, key := range cmdKeys {
		field := strings.TrimSuffix(key, cmdSuffix)
		fieldPath := joinPath(path, field)
		command := node[key].(string)

		if inArray {
			if _, exists := node[field]; exists {
				return fmt.Errorf("%s: set either %q or %q, not both", labelOf(path), field, key)
			}
		} else {
			if l.v.InConfig(fieldPath) {
				return fmt.Errorf("set either %q or %q in the config, not both", fieldPath, fieldPath+cmdSuffix)
			}
			if l.isBindingOverride(fieldPath) {
				// A higher-precedence flag/env binding provides this field, so
				// the command is short-circuited: it never runs. A built-in
				// default does NOT short-circuit it.
				delete(node, key)
				continue
			}
		}

		expanded, err := expandString(command)
		if err != nil {
			return fmt.Errorf("%s: %w", fieldPath, err)
		}
		out, err := runCmd(expanded)
		if err != nil {
			return fmt.Errorf("%s: %w", fieldPath, err)
		}
		delete(node, key)
		node[field] = out
	}

	for k, v := range node {
		switch child := v.(type) {
		case map[string]any:
			if err := l.resolveCmd(child, joinPath(path, k), inArray); err != nil {
				return err
			}
		case []any:
			for i, el := range child {
				if m, ok := el.(map[string]any); ok {
					if err := l.resolveCmd(m, fmt.Sprintf("%s[%d]", joinPath(path, k), i), true); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// runCmd executes command via "sh -c" and returns its trimmed stdout. A missing
// shell or a non-zero exit is fatal; the command's stderr is surfaced, but the
// command string itself (which may carry interpolated secrets) is not.
func runCmd(command string) (string, error) {
	if _, err := exec.LookPath("sh"); err != nil {
		return "", fmt.Errorf("cannot run _cmd: no shell (sh) found in PATH; use env interpolation or a mounted secret file instead")
	}
	cmd := exec.Command("sh", "-c", command)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return "", fmt.Errorf("_cmd failed: %v: %s", err, msg)
		}
		return "", fmt.Errorf("_cmd failed: %v", err)
	}
	return strings.TrimSpace(stdout.String()), nil
}

// firstCmdKey returns the dotted path of the first key ending in "_cmd" found
// anywhere under node, or "" if there is none. Used to fail fast when command
// execution is disabled.
func firstCmdKey(node map[string]any, path string) string {
	for k, v := range node {
		if strings.HasSuffix(k, cmdSuffix) {
			return joinPath(path, k)
		}
		switch child := v.(type) {
		case map[string]any:
			if p := firstCmdKey(child, joinPath(path, k)); p != "" {
				return p
			}
		case []any:
			for i, el := range child {
				if m, ok := el.(map[string]any); ok {
					if p := firstCmdKey(m, fmt.Sprintf("%s[%d]", joinPath(path, k), i)); p != "" {
						return p
					}
				}
			}
		}
	}
	return ""
}

// expandString resolves ${VAR}, ${VAR:-default}, and $$ in s. Other '$'
// characters are literal. A ${VAR} whose variable is unset (and has no default)
// is a fatal error; ${VAR:-default} falls back to default when VAR is unset or
// empty, matching shell ":-" semantics.
func expandString(s string) (string, error) {
	if !strings.ContainsRune(s, '$') {
		return s, nil
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		c := s[i]
		if c != '$' {
			b.WriteByte(c)
			i++
			continue
		}
		// c == '$'
		if i+1 < len(s) && s[i+1] == '$' {
			b.WriteByte('$')
			i += 2
			continue
		}
		if i+1 < len(s) && s[i+1] == '{' {
			end := strings.IndexByte(s[i+2:], '}')
			if end < 0 {
				return "", fmt.Errorf("unterminated ${...} in value")
			}
			val, err := expandExpr(s[i+2 : i+2+end])
			if err != nil {
				return "", err
			}
			b.WriteString(val)
			i = i + 2 + end + 1
			continue
		}
		// Lone '$' (not $$ or ${) is literal.
		b.WriteByte('$')
		i++
	}
	return b.String(), nil
}

// expandExpr resolves the contents of a ${...} reference, i.e. "NAME" or
// "NAME:-default".
func expandExpr(expr string) (string, error) {
	name := expr
	def := ""
	hasDef := false
	if idx := strings.Index(expr, ":-"); idx >= 0 {
		name = expr[:idx]
		def = expr[idx+2:]
		hasDef = true
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("empty variable name in ${%s}", expr)
	}
	if val, ok := os.LookupEnv(name); ok && val != "" {
		return val, nil
	} else if ok && !hasDef {
		// Set but empty, with no default: an explicit empty value.
		return "", nil
	}
	if hasDef {
		return def, nil
	}
	return "", fmt.Errorf("environment variable %q referenced in config is not set", name)
}

func joinPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

func labelOf(path string) string {
	if path == "" {
		return "value"
	}
	return path
}
