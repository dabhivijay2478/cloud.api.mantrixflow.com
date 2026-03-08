export interface ParsedTransformOutput {
  outputColumn: string;
  sourceColumn?: string;
}

const RETURN_DICT_REGEX = /return\s*\{([\s\S]*?)\}/g;
const PAIR_REGEX = /["']?([a-zA-Z_][a-zA-Z0-9_]*)["']?\s*:\s*([^,}]+)/g;
const ASSIGNMENT_REGEX = /^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(.+)$/;

/**
 * Parse transform output columns and best-effort source mappings.
 *
 * This intentionally supports only the simple patterns we allow in the UI:
 * - direct record access in the return dict
 * - local variables assigned from record access earlier in the function
 * - `or` chains such as `record.get("a") or record.get("b")`
 */
export function parseTransformOutputs(script: string): ParsedTransformOutput[] {
  if (!script || typeof script !== "string") {
    return [];
  }

  try {
    const returnMatches = Array.from(script.matchAll(RETURN_DICT_REGEX));
    const returnBody = returnMatches.at(-1)?.[1];
    if (!returnBody) {
      return [];
    }

    const variableSources = parseVariableAssignments(script);
    const outputs: ParsedTransformOutput[] = [];

    for (const pairMatch of returnBody.matchAll(PAIR_REGEX)) {
      const outputColumn = pairMatch[1];
      const valueExpr = pairMatch[2]?.trim() || "";
      const sourceColumn = resolveSourceColumn(valueExpr, variableSources);
      outputs.push(
        sourceColumn ? { outputColumn, sourceColumn } : { outputColumn },
      );
    }

    return outputs;
  } catch {
    return [];
  }
}

/**
 * Extract output column → source column mappings from a transform script.
 * Returns a Map of outputColumnName -> sourceColumnName.
 */
export function parseTransformOutputMappings(script: string): Map<string, string> {
  const result = new Map<string, string>();
  for (const output of parseTransformOutputs(script)) {
    if (output.sourceColumn) {
      result.set(output.outputColumn, output.sourceColumn);
    }
  }
  return result;
}

/**
 * Extract only the output column names returned by the transform.
 */
export function parseTransformOutputColumns(script: string): string[] {
  return parseTransformOutputs(script).map((output) => output.outputColumn);
}

function parseVariableAssignments(script: string): Map<string, string> {
  const result = new Map<string, string>();

  for (const rawLine of script.split("\n")) {
    const line = rawLine.split("#")[0]?.trim();
    if (!line) {
      continue;
    }

    const match = line.match(ASSIGNMENT_REGEX);
    if (!match) {
      continue;
    }

    const variableName = match[1];
    const expression = match[2]?.trim() || "";
    const sourceColumn = resolveSourceColumn(expression, result);
    if (sourceColumn) {
      result.set(variableName, sourceColumn);
    }
  }

  return result;
}

function resolveSourceColumn(
  expr: string,
  variableSources: Map<string, string>,
): string | null {
  const directSource = extractDirectSourceColumn(expr);
  if (directSource) {
    return directSource;
  }

  const identifierMatch = expr.match(/^([a-zA-Z_][a-zA-Z0-9_]*)$/);
  if (!identifierMatch) {
    return null;
  }

  return variableSources.get(identifierMatch[1]) ?? null;
}

/**
 * Extract source column name from a value expression.
 * Supports: record.get("x"), record.get('x'), record["x"], record['x'], record.x
 * and returns the first record-backed column in simple `or` chains.
 */
function extractDirectSourceColumn(expr: string): string | null {
  const getMatch = expr.match(/record\.get\s*\(\s*["']([^"']+)["']\s*\)/);
  if (getMatch) {
    return getMatch[1];
  }

  const bracketMatch = expr.match(/record\s*\[\s*["']([^"']+)["']\s*\]/);
  if (bracketMatch) {
    return bracketMatch[1];
  }

  const dotMatch = expr.match(/record\.([a-zA-Z_][a-zA-Z0-9_]*)\b/);
  if (dotMatch) {
    return dotMatch[1];
  }

  return null;
}
