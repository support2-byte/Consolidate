// middleware/upload.js
import multer from "multer";
import { v2 as cloudinary } from "cloudinary";
import { CloudinaryStorage } from "multer-storage-cloudinary";

// Configure Cloudinary (credentials from .env / Render env vars)
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

const storage = new CloudinaryStorage({
  cloudinary: cloudinary,
  params: (req, file) => {
    // Optional: dynamic folder, e.g. per user or order
    // You can access req.user if auth middleware ran before upload
    const userId = req.user?.id || "anonymous";
    const folder = `consolidate-app/orders/${userId}`;

    // You can customize per field if needed
    let resource_type = "auto"; // handles image / raw (pdf, doc, etc.)

    return {
      folder,
      allowed_formats: [
        "jpg",
        "jpeg",
        "png",
        "gif",
        "pdf",
        "doc",
        "docx",
        "xls",
        "xlsx",
      ],
      resource_type,
      // Optional: public_id: `${file.fieldname}-${Date.now()}`,
    };
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 20 * 1024 * 1024 }, // 20MB
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      "image/jpeg",
      "image/png",
      "image/gif",
      "application/pdf",
      "application/msword",
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      "application/vnd.ms-excel",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ];

    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error("Invalid file type. Allowed: images, PDF, Word, Excel"));
    }
  },
});

export const bugReportUpload = multer({
  storage: new CloudinaryStorage({
    cloudinary,
    params: () => {
      const timestamp = Date.now();
      const today = new Date();
      const dateStr = `${String(today.getDate()).padStart(2, "0")}-${String(today.getMonth() + 1).padStart(2, "0")}-${today.getFullYear()}`;
      return {
        folder: "consolidate-app/bug-report",
        allowed_formats: ["jpg", "jpeg", "png"],
        resource_type: "image",
        public_id: `Bug-Report-${dateStr}-${timestamp}`,
      };
    },
  }),
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = ["image/jpeg", "image/png", "image/jpg"];
    allowed.includes(file.mimetype)
      ? cb(null, true)
      : cb(new Error("Only JPG and PNG images are allowed for bug reports."));
  },
});

export default upload;
