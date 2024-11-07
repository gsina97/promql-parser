package main

import (
	"fmt"
	"regexp"
	"strings"
)

// QueryPart represents a single component of a PromQL query
type QueryPart struct {
	Type     string
	Value    string
	Children []QueryPart // For nested expressions
}

// LabelMatcher represents a label matching expression
type LabelMatcher struct {
	Name     string
	Operator string
	Value    string
}

// VectorSelector represents a vector selector with its labels
type VectorSelector struct {
	MetricName    string
	LabelMatchers []LabelMatcher
	Offset        string
}

// PromQLParser holds the parsing state and results
type PromQLParser struct {
	query           string
	currentPos      int
	parts           []QueryPart
	vectorSelectors []VectorSelector
}

// NewParser creates a new PromQL parser instance
func NewParser(query string) *PromQLParser {
	return &PromQLParser{
		query:           query,
		currentPos:      0,
		parts:           make([]QueryPart, 0),
		vectorSelectors: make([]VectorSelector, 0),
	}
}

// Parse parses the complete PromQL query
func (p *PromQLParser) Parse() ([]QueryPart, error) {
	query := strings.TrimSpace(p.query)

	// Compile all regular expressions
	patterns := map[string]*regexp.Regexp{
		"metric_name": regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*`),
		"function":    regexp.MustCompile(`^(\w+)\(`),
		"label":       regexp.MustCompile(`{([^}]+)}`),
		"operator":    regexp.MustCompile(`^[\+\-\*\/\%\^]=?`),
		"number":      regexp.MustCompile(`^-?\d+(\.\d+)?`),
		"duration":    regexp.MustCompile(`^[0-9]+[smhdwy]`),
		"aggregation": regexp.MustCompile(`^(sum|avg|count|min|max|group)`),
		"by":          regexp.MustCompile(`^by\s*\(`),
		"without":     regexp.MustCompile(`^without\s*\(`),
		"offset":      regexp.MustCompile(`^offset\s+[0-9]+[smhdwy]`),
		"bool":        regexp.MustCompile(`^bool`),
	}

	for len(query) > 0 {
		query = strings.TrimSpace(query)
		if len(query) == 0 {
			break
		}

		matched := false
		for partType, pattern := range patterns {
			if loc := pattern.FindStringIndex(query); loc != nil && loc[0] == 0 {
				match := query[loc[0]:loc[1]]

				switch partType {
				case "metric_name":
					// Check if it's followed by label matchers
					if strings.HasPrefix(query[loc[1]:], "{") {
						vs, remaining := p.parseVectorSelector(query)
						p.vectorSelectors = append(p.vectorSelectors, vs)
						p.parts = append(p.parts, QueryPart{
							Type:     "vector_selector",
							Value:    vs.MetricName,
							Children: p.convertLabelsToQueryParts(vs.LabelMatchers),
						})
						query = remaining
					} else {
						p.parts = append(p.parts, QueryPart{
							Type:  "metric_name",
							Value: match,
						})
						query = query[loc[1]:]
					}

				case "function":
					funcName := strings.TrimSuffix(match, "(")
					args, remaining := p.parseFunctionArgs(query[loc[1]:])
					p.parts = append(p.parts, QueryPart{
						Type:     "function",
						Value:    funcName,
						Children: args,
					})
					query = remaining

				case "by":
					grouping, remaining := p.parseGrouping(query[loc[1]:])
					p.parts = append(p.parts, QueryPart{
						Type:     "by",
						Value:    "by",
						Children: grouping,
					})
					query = remaining

				case "label":
					labelMatchers := p.parseLabelMatchers(match)
					p.parts = append(p.parts, QueryPart{
						Type:     "labels",
						Children: p.convertLabelsToQueryParts(labelMatchers),
					})
					query = query[loc[1]:]

				default:
					p.parts = append(p.parts, QueryPart{
						Type:  partType,
						Value: match,
					})
					query = query[loc[1]:]
				}

				matched = true
				break
			}
		}

		if !matched {
			// Handle special characters and move forward
			if query[0] == '(' || query[0] == ')' || query[0] == ',' || query[0] == '[' || query[0] == ']' {
				p.parts = append(p.parts, QueryPart{
					Type:  "delimiter",
					Value: string(query[0]),
				})
			}
			query = query[1:]
		}
	}

	return p.parts, nil
}

// parseVectorSelector parses a metric name and its label matchers
func (p *PromQLParser) parseVectorSelector(query string) (VectorSelector, string) {
	metricPattern := regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)`)
	match := metricPattern.FindStringSubmatch(query)

	if len(match) == 0 {
		return VectorSelector{}, query
	}

	metricName := match[1]
	remaining := query[len(metricName):]

	// Parse label matchers if present
	if strings.HasPrefix(remaining, "{") {
		labelStr := p.extractLabelString(remaining)
		labelMatchers := p.parseLabelMatchers(labelStr)
		remaining = remaining[len(labelStr):]

		// Parse offset if present
		offset := ""
		if strings.HasPrefix(remaining, " offset ") {
			offsetPattern := regexp.MustCompile(`offset\s+([0-9]+[smhdwy])`)
			if offsetMatch := offsetPattern.FindStringSubmatch(remaining); len(offsetMatch) > 1 {
				offset = offsetMatch[1]
				remaining = remaining[len(offsetMatch[0]):]
			}
		}

		return VectorSelector{
			MetricName:    metricName,
			LabelMatchers: labelMatchers,
			Offset:        offset,
		}, remaining
	}

	return VectorSelector{
		MetricName: metricName,
	}, remaining
}

// parseFunctionArgs parses function arguments
func (p *PromQLParser) parseFunctionArgs(query string) ([]QueryPart, string) {
	args := make([]QueryPart, 0)
	parenthesesCount := 1
	currentArg := ""

	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '(':
			parenthesesCount++
			currentArg += string(query[i])
		case ')':
			parenthesesCount--
			if parenthesesCount == 0 {
				if currentArg != "" {
					subParser := NewParser(currentArg)
					subParts, _ := subParser.Parse()
					args = append(args, subParts...)
				}
				return args, query[i+1:]
			}
			currentArg += string(query[i])
		case ',':
			if parenthesesCount == 1 {
				if currentArg != "" {
					subParser := NewParser(currentArg)
					subParts, _ := subParser.Parse()
					args = append(args, subParts...)
				}
				currentArg = ""
			} else {
				currentArg += string(query[i])
			}
		default:
			currentArg += string(query[i])
		}
	}

	return args, ""
}

// parseGrouping parses "by" and "without" clauses
func (p *PromQLParser) parseGrouping(query string) ([]QueryPart, string) {
	grouping := make([]QueryPart, 0)
	parenthesesCount := 1
	current := ""

	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '(':
			parenthesesCount++
		case ')':
			parenthesesCount--
			if parenthesesCount == 0 {
				if current != "" {
					labels := strings.Split(current, ",")
					for _, label := range labels {
						label = strings.TrimSpace(label)
						if label != "" {
							grouping = append(grouping, QueryPart{
								Type:  "group_label",
								Value: label,
							})
						}
					}
				}
				return grouping, query[i+1:]
			}
		default:
			if parenthesesCount == 1 && query[i] != '(' {
				current += string(query[i])
			}
		}
	}

	return grouping, query
}

// parseLabelMatchers parses label matchers inside curly braces
func (p *PromQLParser) parseLabelMatchers(labelStr string) []LabelMatcher {
	// Remove outer braces
	labelStr = strings.Trim(labelStr, "{}")

	matchers := make([]LabelMatcher, 0)
	parts := strings.Split(labelStr, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Match label name, operator, and value
		pattern := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!=|=|!~)\s*"([^"]*)"`)
		matches := pattern.FindStringSubmatch(part)

		if len(matches) == 4 {
			matchers = append(matchers, LabelMatcher{
				Name:     matches[1],
				Operator: matches[2],
				Value:    matches[3],
			})
		}
	}

	return matchers
}

// convertLabelsToQueryParts converts label matchers to QueryParts
func (p *PromQLParser) convertLabelsToQueryParts(matchers []LabelMatcher) []QueryPart {
	parts := make([]QueryPart, 0)

	for _, matcher := range matchers {
		parts = append(parts, QueryPart{
			Type:  "label_matcher",
			Value: fmt.Sprintf("%s%s\"%s\"", matcher.Name, matcher.Operator, matcher.Value),
		})
	}

	return parts
}

// extractLabelString extracts the complete label string from a query
func (p *PromQLParser) extractLabelString(query string) string {
	if !strings.HasPrefix(query, "{") {
		return ""
	}

	braceCount := 0
	for i, char := range query {
		if char == '{' {
			braceCount++
		} else if char == '}' {
			braceCount--
			if braceCount == 0 {
				return query[:i+1]
			}
		}
	}
	return query
}

// Example usage function
func main() {
	queries := []string{
		`rate(http_requests_total{job="api-server", method="POST"}[5m])`,
		`sum(http_requests_total) by (method, code)`,
		`http_requests_total{status!="500"} / http_requests_total`,
		`max_over_time(node_cpu_seconds_total{mode="idle"}[1h]) by (instance)`,
		`sum(rate(container_cpu_usage_seconds_total{container!=""}[5m])) by (namespace)`,
		`(avg by (cluster_id, node, database) (azure_cpu_percent))`,
	}

	for _, query := range queries {
		parser := NewParser(query)
		parts, err := parser.Parse()
		if err != nil {
			fmt.Printf("Error parsing query: %v\n", err)
			continue
		}

		fmt.Printf("\nQuery: %s\n", query)
		printQueryParts(parts, 0)
	}
}

// printQueryParts prints the query parts with proper indentation
func printQueryParts(parts []QueryPart, indent int) {
	indentStr := strings.Repeat("  ", indent)

	for _, part := range parts {
		fmt.Printf("%s%s: %s\n", indentStr, part.Type, part.Value)
		if len(part.Children) > 0 {
			printQueryParts(part.Children, indent+1)
		}
	}
}
