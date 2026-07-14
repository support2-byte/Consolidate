import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TEMPLATES_DIR = path.join(__dirname, "..", "templates");

const cache = new Map();

function loadTemplate(name) {
  if (cache.has(name)) return cache.get(name);
  const filePath = path.join(TEMPLATES_DIR, name);
  const content = fs.readFileSync(filePath, "utf8");
  cache.set(name, content);
  return content;
}

export function renderTemplate(name, data = {}) {
  const template = loadTemplate(name);
  return template.replace(/<\?=\s*(\w+)\s*\?>/g, (_match, key) => {
    return Object.prototype.hasOwnProperty.call(data, key)
      ? String(data[key])
      : "";
  });
}
