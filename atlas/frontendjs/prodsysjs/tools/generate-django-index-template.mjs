import { readFile, writeFile, unlink } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');

const candidateIndexPaths = [
  path.resolve(projectRoot, '../static/js/index.html'),
  path.resolve(projectRoot, '../static/js/browser/index.html'),
];

const djangoTemplatePath = path.resolve(projectRoot, '../templates/frontendjs/_ng_template.html');

const djangoScriptTemplate = (scriptName, scriptType = 'module') =>
  `<script type="${scriptType}" src="{% static "js/${scriptName}" %}"></script>`;
const djangoCSSTemplate = (styleName) =>
  `<link rel="stylesheet" href="{% static "js/${styleName}" %}">`;

function normalizeAssetPath(assetPath) {
  return assetPath.replace(/^\/+/, '').replace(/^\.\//, '');
}

function buildDjangoTemplate(indexHtml) {
  const styles = [];
  const scripts = [];

  const styleRegex = /<link[^>]*rel=["']stylesheet["'][^>]*href=["']([^"']+)["'][^>]*>/gi;
  const scriptRegex = /<script[^>]*src=["']([^"']+)["'][^>]*><\/script>/gi;

  let styleMatch;
  while ((styleMatch = styleRegex.exec(indexHtml)) !== null) {
    styles.push(normalizeAssetPath(styleMatch[1]));
  }

  let scriptMatch;
  while ((scriptMatch = scriptRegex.exec(indexHtml)) !== null) {
    scripts.push(normalizeAssetPath(scriptMatch[1]));
  }

  const lines = ['{% load static  %}'];
  for (const styleName of styles) lines.push(djangoCSSTemplate(styleName));
  lines.push('<app-root></app-root>');
  for (const scriptName of scripts) lines.push(djangoScriptTemplate(scriptName));

  return `${lines.join('\n')}\n`;
}

async function readBuiltIndex() {
  for (const indexPath of candidateIndexPaths) {
    try {
      const indexHtml = await readFile(indexPath, 'utf8');
      return { indexHtml, indexPath };
    } catch {
      // Try next candidate path.
    }
  }
  throw new Error(`Cannot find built index.html. Tried: ${candidateIndexPaths.join(', ')}`);
}

const { indexHtml } = await readBuiltIndex();
const djangoTemplate = buildDjangoTemplate(indexHtml);

await writeFile(djangoTemplatePath, djangoTemplate, 'utf8');

// Cleanup: remove generated Angular index files (if present)
for (const indexPath of candidateIndexPaths) {
  try {
    await unlink(indexPath);
  } catch {
    // Ignore if file does not exist.
  }
}

console.log(`Django Angular template generated: ${djangoTemplatePath}`);

