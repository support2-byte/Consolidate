import multer from "multer";
import path from "path";
import fs from "fs";

const uploadDir = path.join(process.cwd(), "uploads");

// Ensure uploads directory exists
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

// Configure storage
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    console.log("Multer destination:", uploadDir);
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    const filename = `${req.params.zoho_id}-${file.originalname}`;
    console.log("Multer saving file:", { filename, mimetype: file.mimetype });
    cb(null, filename);
  },
});

// Allowed file types
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

// Configure multer
const upload = multer({
  storage,
  limits: { fileSize: 20 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    console.log("Multer fileFilter:", { originalname: file.originalname, mimetype: file.mimetype });
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      console.error("Multer fileFilter rejected file:", file.mimetype);
      cb(new Error("Only images, PDF, Word, and Excel files are allowed"));
    }
  },
});

console.log("Multer configured with diskStorage");
export default upload; // Ensure default export