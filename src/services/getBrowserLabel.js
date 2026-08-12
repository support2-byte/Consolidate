import { UAParser } from "ua-parser-js";

export function getBrowserLabel(userAgentString) {
  const parser = new UAParser(userAgentString);
  const { name: browserName } = parser.getBrowser();
  const { name: osName } = parser.getOS();

  if (!browserName && !osName) return "unknown";
  if (browserName && osName) return `${browserName} on ${osName}`;
  return browserName || osName || "unknown";
}
