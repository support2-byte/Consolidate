import { EMAIL_TEMPLATES } from "../templates/email_templates.js";

export function renderEmail(name, data = {}) {
  const template = EMAIL_TEMPLATES[name];
  if (!template) throw new Error(`Unknown email template: ${name}`);

  const values = { year: new Date().getFullYear(), ...data };
  return template.replace(/{{(\w+)}}/g, (_match, key) =>
    values[key] === undefined || values[key] === null
      ? ""
      : String(values[key]),
  );
}
