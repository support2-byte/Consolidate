import express from "express";
import multer from "multer";
import {
  createCustomer,
  getCustomerById,
  getCustomers,
  saveContacts,
  deleteContact,
  uploadDocument,
  deleteDocument,
  updateCustomer,
  getDocuments,
  downloadDocument,
  updateDocument,
  deleteCustomer,
  getCustomersPanel
} from "./customer.controller.js"; // Adjust path as needed
import upload from "../../middleware/upload.js";
import { get } from "http";
const router = express.Router();
// const upload = multer({ storage: multer.memoryStorage() });

router.post("/", createCustomer);
router.get("/:id", getCustomerById);
router.get("/", getCustomers);
router.get("/panel", getCustomersPanel);

router.put("/:zoho_id", updateCustomer);
router.delete("/:zoho_id", deleteCustomer);
// Contact routes
router.post("/:zoho_id/contacts", saveContacts); // POST /api/customers/:zoho_id/contacts
router.delete("/:zoho_id/contacts/:contact_person_id", deleteContact); // DELETE /api/customers/:zoho_id/contacts/:contact_person_id

// Document routes
router.post("/:zoho_id/documents", (req, res, next) => {
  console.log("Raw request body fields:", Object.keys(req.body));
  console.log("Raw request files:", req.file);
  next();
}, upload.single("file"), uploadDocument);
router.get('/:zoho_id/documents', getDocuments);
router.put('/:zoho_id/documents/:document_id', updateDocument);
router.delete("/:zoho_id/documents/:document_id", deleteDocument);
router.get('/:zoho_id/documents/:document_id/download', downloadDocument);

export default router;