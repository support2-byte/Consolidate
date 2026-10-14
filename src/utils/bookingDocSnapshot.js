import puppeteer from "puppeteer";
import streamifier from "streamifier";
import cloudinary from "../services/cloudinary.js";

/**
 * @param {string} html
 * @returns {Promise<Buffer>}
 */
export async function renderHtmlToPdf(html) {
  const browser = await puppeteer.launch({
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });
  try {
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle0", timeout: 15000 });
    const pdfBuffer = await page.pdf({
      format: "A4",
      printBackground: true,
      margin: { top: "20px", bottom: "20px", left: "20px", right: "20px" },
    });
    return pdfBuffer;
  } finally {
    await browser.close();
  }
}

/**
 * @param {Buffer} buffer
 * @param {import("cloudinary").UploadApiOptions} options
 * @returns {Promise<import("cloudinary").UploadApiResponse>}
 */
export function uploadBufferToCloudinary(buffer, options) {
  return new Promise((resolve, reject) => {
    const uploadStream = cloudinary.uploader.upload_stream(
      options,
      (err, result) => {
        if (err) return reject(err);
        resolve(result);
      },
    );
    streamifier.createReadStream(buffer).pipe(uploadStream);
  });
}

/**
 * @param {string} html
 * @param {string} formId
 * @returns {Promise<string>}
 */
export async function generateAndUploadBookingSnapshot(html, formId) {
  const pdfBuffer = await renderHtmlToPdf(html);

  const publicIdWithExt = `${formId}.pdf`;

  const result = await uploadBufferToCloudinary(pdfBuffer, {
    folder: "consolidate-app/booking-confirmation",
    public_id: publicIdWithExt,
    resource_type: "raw",
    type: "authenticated",
    overwrite: true,
  });

  const signedUrl = cloudinary.url(result.public_id, {
    resource_type: "raw",
    type: "authenticated",
    sign_url: true,
    version: result.version,
  });

  return signedUrl;
}
