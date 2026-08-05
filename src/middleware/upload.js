import multer from "multer";
import { v2 as cloudinary } from "cloudinary";
import { CloudinaryStorage } from "multer-storage-cloudinary";

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

const storage = new CloudinaryStorage({
  cloudinary: cloudinary,
  params: (req, file) => {
    const userId = req.user?.id || "anonymous";
    const folder = `consolidate-app/orders/${userId}`;

    let resource_type = "auto";

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
    };
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 20 * 1024 * 1024 },
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

const getFileLabel = (mimetype) => {
  if (mimetype.startsWith("image/")) return "Image";
  if (mimetype === "application/pdf") return "PDF";
  if (mimetype.includes("word")) return "Doc";
  if (mimetype.includes("excel") || mimetype.includes("spreadsheet"))
    return "Excel";
  return "File";
};

const EXT_BY_MIME = {
  "image/jpeg": "jpg",
  "image/png": "png",
  "image/gif": "gif",
  "application/pdf": "pdf",
  "application/msword": "doc",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
    "docx",
  "application/vnd.ms-excel": "xls",
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
};

export const containerUpload = multer({
  storage: new CloudinaryStorage({
    cloudinary,
    params: (req, file) => {
      const isEdit = !!req.params?.cid;
      const containerNumber = (
        req.body?.container_number ||
        req.body?.containerNo ||
        "UNKNOWN"
      )
        .toString()
        .toUpperCase()
        .replace(/\s+/g, "");
      const label = getFileLabel(file.mimetype);
      const dateStr = new Date().toISOString().slice(0, 10);

      const folder = isEdit
        ? `consolidate-app/containers/${containerNumber}`
        : `consolidate-app/containers/new`;

      const isImage = file.mimetype.startsWith("image/");
      const ext = EXT_BY_MIME[file.mimetype] || "bin";
      const basePublicId = `${containerNumber}_${label}_${dateStr}_${Date.now()}`;

      return {
        folder,
        public_id: isImage ? basePublicId : `${basePublicId}.${ext}`,
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
        resource_type: isImage ? "image" : "raw",
        type: isImage ? "upload" : "authenticated",
      };
    },
  }),
  limits: { fileSize: 5 * 1024 * 1024, files: 5 },
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
    if (allowedTypes.includes(file.mimetype)) cb(null, true);
    else cb(new Error("Invalid file type. Allowed: images, PDF, Word, Excel"));
  },
}).array("files", 5);

export const kycUpload = multer({
  storage: new CloudinaryStorage({
    cloudinary,
    params: () => {
      return {
        folder: "consolidate-app/kyc",
        allowed_formats: ["jpg", "jpeg", "png", "pdf"],
        resource_type: "auto",
      };
    },
  }),
  limits: {
    fileSize: 5 * 1024 * 1024,
    files: 3,
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = ["image/jpeg", "image/png", "application/pdf"];
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error("Invalid file type. Allowed: JPG, PNG, PDF"));
    }
  },
}).fields([
  { name: "passport", maxCount: 1 },
  { name: "emiratesId", maxCount: 1 },
  { name: "signature", maxCount: 1 },
]);

export const gatepassUpload = multer({
  storage: new CloudinaryStorage({
    cloudinary,
    params: (req, file) => {
      const formNo = (
        req.body?.rgl_booking_number ||
        req.params?.orderId ||
        "UNKNOWN"
      )
        .toString()
        .toUpperCase()
        .replace(/\s+/g, "");

      const match = file.fieldname.match(/^gatepass_(.+)$/);
      const receiverId = match ? match[1] : "unknown";

      const ext = EXT_BY_MIME[file.mimetype] || "bin";
      const isImage = file.mimetype.startsWith("image/");
      const basePublicId = `${formNo}_${receiverId}_${Date.now()}`;

      return {
        folder: "consolidate-app/orders/gatepass",
        public_id: isImage ? basePublicId : `${basePublicId}.${ext}`,
        allowed_formats: ["jpg", "jpeg", "png", "pdf"],
        resource_type: isImage ? "image" : "raw",
        type: isImage ? "upload" : "authenticated",
      };
    },
  }),
  limits: { fileSize: 5 * 1024 * 1024, files: 20 },
  fileFilter: (req, file, cb) => {
    const allowedTypes = ["image/jpeg", "image/png", "application/pdf"];
    if (allowedTypes.includes(file.mimetype)) cb(null, true);
    else cb(new Error("Invalid file type. Allowed: JPG, PNG, PDF"));
  },
}).any();

export default upload;
