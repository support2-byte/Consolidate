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
  getCustomersPanel,
} from "./customer.controller.js"; // Adjust path as needed
import upload from "../../middleware/upload.js";
import { get } from "http";
const router = express.Router();
// const upload = multer({ storage: multer.memoryStorage() });

/**
 * @swagger
 * /api/customers:
 *   post:
 *     summary: Create a new customer
 *     tags: [Customers]
 *     security:
 *       - cookieAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/CreateCustomerRequest'
 *     responses:
 *       201:
 *         description: Customer created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Customer'
 *       400:
 *         description: Invalid input
 */
router.post("/", createCustomer);

/**
 * @swagger
 * /api/customers/{id}:
 *   get:
 *     summary: Get a customer by ID
 *     tags: [Customers]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Customer found
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Customer'
 *       404:
 *         description: Customer not found
 */
router.get("/:id", getCustomerById);

/**
 * @swagger
 * /api/customers:
 *   get:
 *     summary: List all customers
 *     tags: [Customers]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: List of customers
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Customer'
 */
router.get("/", getCustomers);

/**
 * @swagger
 * /api/customers/customerpanel:
 *   get:
 *     summary: Get customer panel/summary data
 *     tags: [Customers]
 *     security:
 *       - cookieAuth: []
 *     responses:
 *       200:
 *         description: Customer panel data
 */
router.get("/customerpanel", getCustomersPanel);

/**
 * @swagger
 * /api/customers/{zoho_id}:
 *   put:
 *     summary: Update a customer
 *     tags: [Customers]
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
 *             $ref: '#/components/schemas/UpdateCustomerRequest'
 *     responses:
 *       200:
 *         description: Customer updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Customer'
 *       404:
 *         description: Customer not found
 */
router.put("/:zoho_id", updateCustomer);

/**
 * @swagger
 * /api/customers/{zoho_id}:
 *   delete:
 *     summary: Delete a customer
 *     tags: [Customers]
 *     security:
 *       - cookieAuth: []
 *     parameters:
 *       - in: path
 *         name: zoho_id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Customer deleted
 *       404:
 *         description: Customer not found
 */
router.delete("/:zoho_id", deleteCustomer);

// Contact routes

/**
 * @swagger
 * /api/customers/{zoho_id}/contacts:
 *   post:
 *     summary: Save/replace contacts for a customer
 *     tags: [Customer Contacts]
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
 *             $ref: '#/components/schemas/SaveContactsRequest'
 *     responses:
 *       200:
 *         description: Contacts saved
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Contact'
 *       404:
 *         description: Customer not found
 */
router.post("/:zoho_id/contacts", saveContacts); // POST /api/customers/:zoho_id/contacts

/**
 * @swagger
 * /api/customers/{zoho_id}/contacts/{contact_person_id}:
 *   delete:
 *     summary: Delete a customer contact
 *     tags: [Customer Contacts]
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
router.delete("/:zoho_id/contacts/:contact_person_id", deleteContact); // DELETE /api/customers/:zoho_id/contacts/:contact_person_id

// Document routes

/**
 * @swagger
 * /api/customers/{zoho_id}/documents:
 *   post:
 *     summary: Upload a document for a customer
 *     tags: [Customer Documents]
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
 *               $ref: '#/components/schemas/CustomerDocument'
 *       400:
 *         description: Invalid file or missing file
 *       404:
 *         description: Customer not found
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
 * /api/customers/{zoho_id}/documents:
 *   get:
 *     summary: List documents for a customer
 *     tags: [Customer Documents]
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
 *                 $ref: '#/components/schemas/CustomerDocument'
 *       404:
 *         description: Customer not found
 */
router.get("/:zoho_id/documents", getDocuments);

/**
 * @swagger
 * /api/customers/{zoho_id}/documents/{document_id}:
 *   put:
 *     summary: Update a customer document's metadata
 *     tags: [Customer Documents]
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
 *             $ref: '#/components/schemas/UpdateDocumentRequest'
 *     responses:
 *       200:
 *         description: Document updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/CustomerDocument'
 *       404:
 *         description: Document not found
 */
router.put("/:zoho_id/documents/:document_id", updateDocument);

/**
 * @swagger
 * /api/customers/{zoho_id}/documents/{document_id}:
 *   delete:
 *     summary: Delete a customer document
 *     tags: [Customer Documents]
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
 * /api/customers/{zoho_id}/documents/{document_id}/download:
 *   get:
 *     summary: Download a customer document
 *     tags: [Customer Documents]
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
