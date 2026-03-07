/**
 * Minimal parser for Python transform scripts.
 * Extracts output column → source column mappings from the return dict.
 *
 * Supports patterns:
 * - record.get("column_name") / record.get('column_name')
 * - record["column_name"] / record['column_name']
 * - record.column_name
 */

/**
 * Extract output column → source column mappings from a transform script.
 * Returns a Map of outputColumnName -> sourceColumnName.
 * Returns empty Map if parsing fails.
 */
export function parseTransformOutputMappings(script: string): Map<string, string> {
  const result = new Map<string, string>();

  if (!script || typeof script !== 'string') {
    return result;
  }

  try {
    // Find the return statement with a dict: return { ... } or return {...}
    const returnMatch = script.match(/return\s*\{([^}]*)\}/s);
    if (!returnMatch) {
      return result;
    }

    const returnBody = returnMatch[1];

    // Match key-value pairs: "key": value or 'key': value
    // Key can be quoted or unquoted identifier
    const pairRegex = /["']?([a-zA-Z_][a-zA-Z0-9_]*)["']?\s*:\s*([^,}]+)/g;
    let pairMatch;

    while ((pairMatch = pairRegex.exec(returnBody)) !== null) {
      const outputKey = pairMatch[1];
      const valueExpr = pairMatch[2].trim();

      const sourceCol = extractSourceColumn(valueExpr);
      if (sourceCol) {
        result.set(outputKey, sourceCol);
      }
    }
  } catch {
    return result;
  }

  return result;
}

/**
 * Extract source column name from a value expression.
 * Supports: record.get("x"), record.get('x'), record["x"], record['x'], record.x
 */
function extractSourceColumn(expr: string): string | null {
  // record.get("column") or record.get('column')
  const getMatch = expr.match(/record\.get\s*\(\s*["']([^"']+)["']\s*\)/);
  if (getMatch) {
    return getMatch[1];
  }

  // record["column"] or record['column']
  const bracketMatch = expr.match(/record\s*\[\s*["']([^"']+)["']\s*\]/);
  if (bracketMatch) {
    return bracketMatch[1];
  }

  // record.column (simple attribute access)
  const dotMatch = expr.match(/record\.([a-zA-Z_][a-zA-Z0-9_]*)\b/);
  if (dotMatch) {
    return dotMatch[1];
  }

  return null;
}
