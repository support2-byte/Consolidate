import { Router } from "express";
import * as customerController from "./customer.controller.js";
import * as contactController from "./contact.controller.js";
import * as documentController from "./document.controller.js";
import { auth } from "../auth/auth.middleware.js";
import multer from "multer";
import path from "path";
import fs from "fs";

// Ensure uploads folder exists
const uploadDir = path.join(process.cwd(), "uploads");
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir);
}
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadDir),
  filename: (req, file, cb) =>
    cb(null, Date.now() + "-" + file.originalname)
});
const upload = multer({ storage });

const router = Router();

// Customers
router.get("/", auth, customerController.getCustomers);
router.get("/:id", auth, customerController.getCustomerById);
router.put("/:id", auth, customerController.updateCustomer);
router.delete("/:id", auth, customerController.deleteCustomer);
router.post("/", auth, customerController.createCustomer);

// Contacts
router.get("/:id/contacts", auth, contactController.getContacts);
router.post("/:id/contacts", auth, contactController.saveContacts);
router.delete("/:id/contacts/:contactId", auth, contactController.deleteContact);

// Documents
router.get("/:id/documents", auth, documentController.getDocuments);
router.post("/:id/documents", auth, upload.single("file"), documentController.uploadDocument);
router.delete("/:id/documents/:docId", auth, documentController.deleteDocument);

export default router;
