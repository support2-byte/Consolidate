import express from "express";
import multer from "multer";
import {
  createCustomer,
  getCustomerById,
  getvendors,
  saveContacts,
  deleteContact,
  uploadDocument,
  deleteDocument,
  updateCustomer,
  getDocuments,
  downloadDocument,
  updateDocument,
  deleteCustomer,
} from "./vendorController.js";
import upload from "../../middleware/upload.js";
const router = express.Router();
// const upload = multer({ storage: multer.memoryStorage() });

/**
 * @swagger
 * /api/vendors:
 *   post:
 *     summary: Create a new vendor
 *     tags: [Vendors]
 *     security:
 *       - cookieAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/CreateVendorRequest'
 *     responses:
 *       201:
 *         description: Vendor created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Vendor'
 *       400:
 *         description: Invalid input
 */
router.post("/", createCustomer);

/**
 * @swagger
 * /api/vendors/{id}:
 *   get:
 *     summary: Get a vendor by ID
 *     tags: [Vendors]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Vendor found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Vendor'
 *       404:
 *         description: Vendor not found
 */
router.get("/:id", getCustomerById);

/**
 * @swagger
 * /api/vendors:
 *   get:
 *     summary: List all vendors
 *     tags: [Vendors]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of vendors
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Vendor'
 */
router.get("/", getvendors);

/**
 * @swagger
 * /api/vendors/{zoho_id}:
 *   put:
 *     summary: Update a vendor
 *     tags: [Vendors]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: zoho_id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/UpdateVendorRequest'
 *     responses:
 *       200:
 *         description: Vendor updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Vendor'
 *       404:
 *         description: Vendor not found
 */
router.put("/:zoho_id", updateCustomer);

/**
 * @swagger
 * /api/vendors/{zoho_id}:
 *   delete:
 *     summary: Delete a vendor
 *     tags: [Vendors]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: zoho_id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Vendor deleted
 *       404:
 *         description: Vendor not found
 */
router.delete("/:zoho_id", deleteCustomer);

// Contact routes

/**
 * @swagger
 * /api/vendors/{zoho_id}/contacts:
 *   post:
 *     summary: Save/replace contacts for a vendor
 *     tags: [Vendor Contacts]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: zoho_id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/SaveVendorContactsRequest'
 *     responses:
 *       200:
 *         description: Contacts saved
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/VendorContact'
 *       404:
 *         description: Vendor not found
 */
router.post("/:zoho_id/contacts", saveContacts);

/**
 * @swagger
 * /api/vendors/{zoho_id}/contacts/{contact_person_id}:
 *   delete:
 *     summary: Delete a vendor contact
 *     tags: [Vendor Contacts]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: zoho_id
 *         required: true
 *         schema: { type: string }
 *       - in: path
 *         name: contact_person_id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Contact deleted
 *       404:
 *         description: Contact not found
 */
router.delete("/:zoho_id/contacts/:contact_person_id", deleteContact);

// Document routes

/**
 * @swagger
 * /api/vendors/{zoho_id}/documents:
 *   post:
 *     summary: Upload a document for a vendor
 *     tags: [Vendor Documents]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: zoho_id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             type: object
 *             properties:
 *               file:
 *                 type: string
 *                 format: binary
 *     responses:
 *       201:
 *         description: Document uploaded
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/VendorDocument'
 *       400:
 *         description: Invalid file or missing file
 *       404:
 *         description: Vendor not found
 */
router.post(
  "/:zoho_id/documents",
  (req, res, next) => {
    console.log("Raw request body fields:", Object.keys(req.body));
    console.log("Raw request files:", req.file);
    next();
  },
  upload.single("file"),
  uploadDocument,
);

/**
 * @swagger
 * /api/vendors/{zoho_id}/documents:
 *   get:
 *     summary: List documents for a vendor
 *     tags: [Vendor Documents]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: zoho_id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: List of documents
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/VendorDocument'
 *       404:
 *         description: Vendor not found
 */
router.get("/:zoho_id/documents", getDocuments);

/**
 * @swagger
 * /api/vendors/{zoho_id}/documents/{document_id}:
 *   put:
 *     summary: Update a vendor document's metadata
 *     tags: [Vendor Documents]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: zoho_id
 *         required: true
 *         schema: { type: string }
 *       - in: path
 *         name: document_id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/UpdateVendorDocumentRequest'
 *     responses:
 *       200:
 *         description: Document updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/VendorDocument'
 *       404:
 *         description: Document not found
 */
router.put("/:zoho_id/documents/:document_id", updateDocument);

/**
 * @swagger
 * /api/vendors/{zoho_id}/documents/{document_id}:
 *   delete:
 *     summary: Delete a vendor document
 *     tags: [Vendor Documents]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: zoho_id
 *         required: true
 *         schema: { type: string }
 *       - in: path
 *         name: document_id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Document deleted
 *       404:
 *         description: Document not found
 */
router.delete("/:zoho_id/documents/:document_id", deleteDocument);

/**
 * @swagger
 * /api/vendors/{zoho_id}/documents/{document_id}/download:
 *   get:
 *     summary: Download a vendor document
 *     tags: [Vendor Documents]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: zoho_id
 *         required: true
 *         schema: { type: string }
 *       - in: path
 *         name: document_id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: File stream
 *         content:
 *           application/octet-stream:
 *             schema:
 *               type: string
 *               format: binary
 *       404:
 *         description: Document not found
 */
router.get("/:zoho_id/documents/:document_id/download", downloadDocument);

export default router;
