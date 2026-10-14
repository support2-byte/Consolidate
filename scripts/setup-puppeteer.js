import { execSync } from "child_process";

try {
  execSync("npx puppeteer browsers install chrome", { stdio: "inherit" });
  console.log("✔ Puppeteer Chrome installed.");
} catch (err) {
  console.warn(
    "⚠ Puppeteer Chrome install failed during postinstall. " +
      "PDF snapshot generation will not work until this is resolved manually " +
      "by running: npx puppeteer browsers install chrome",
  );
}
