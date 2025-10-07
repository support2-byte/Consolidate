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
  const type = file.fieldname; // e.g., 'attachments'
  const dir = path.join(uploadDir, type);
  fs.mkdirSync(dir, { recursive: true });
  cb(null, dir);
},
 filename: (req, file, cb) => {
  const timestamp = Date.now();
  const ext = path.extname(file.originalname);
  const baseName = path.basename(file.originalname, ext);
  const filename = `${timestamp}-${baseName}${ext}`;
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